// Package base_repo file: repo_impl.go

package base_repo

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// RepoImpl is a generic, thread-safe, in-memory repository.
type RepoImpl[T Entity] struct {
	store         sync.Map
	indexes       map[string]Indexer[T]
	subscribers   []chan Event[T]
	mu            sync.RWMutex
	subscribersMu sync.RWMutex
	logEnabled    bool
	logger        *slog.Logger
}

// NewRepo creates a new repository with logging disabled.
func NewRepo[T Entity]() *RepoImpl[T] {
	return &RepoImpl[T]{
		indexes:    make(map[string]Indexer[T]),
		logEnabled: false,
		logger:     slog.Default(),
	}
}

// NewRepoWithLogs enables logging for the repository.
func NewRepoWithLogs[T Entity]() *RepoImpl[T] {
	return &RepoImpl[T]{
		indexes:    make(map[string]Indexer[T]),
		logEnabled: true,
		logger:     slog.Default(),
	}
}

// entityAttrs returns a consistent set of slog.Attrs for an entity.
func (r *RepoImpl[T]) entityAttrs(obj T) slog.Attr {
	return slog.Group("entity", "id", obj.GetBase().ID, "name", obj.EntityName())
}

// The following methods have been updated with structured logging.

func (r *RepoImpl[T]) notify(eventType string, obj T) {
	if r.logEnabled {
		r.logger.Info("Notifying subscribers", "eventType", eventType, r.entityAttrs(obj))
	}
	r.subscribersMu.RLock()
	defer r.subscribersMu.RUnlock()

	for _, ch := range r.subscribers {
		ev := Event[T]{Type: eventType, Obj: obj}
		ch <- ev
	}
}

func (r *RepoImpl[T]) rollback(ctx context.Context, updatedIndexes map[string]Indexer[T], obj T) {
	if r.logEnabled {
		rollbackLogger := r.logger.With("op", "rollback", r.entityAttrs(obj))
		rollbackLogger.InfoContext(ctx, "Starting index rollback")
		for name, rollbackIdx := range updatedIndexes {
			rollbackIdx.Delete(ctx, obj)
			rollbackLogger.InfoContext(ctx, "Rolled back index", "indexName", name)
		}
	} else {
		for _, rollbackIdx := range updatedIndexes {
			rollbackIdx.Delete(ctx, obj)
		}
	}
}

func (r *RepoImpl[T]) insertIndexes(ctx context.Context, obj T) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.logEnabled {
		r.logger.InfoContext(ctx, "Attempting to insert into indexes", r.entityAttrs(obj), "numIndexes", len(r.indexes))
	}

	updatedIndexes := make(map[string]Indexer[T])
	for name, idx := range r.indexes {
		if err := idx.Insert(ctx, obj); err != nil {
			if r.logEnabled {
				r.logger.ErrorContext(ctx, "Failed to insert into index, beginning rollback", "indexName", name, r.entityAttrs(obj), "error", err)
			}
			r.rollback(ctx, updatedIndexes, obj)
			return fmt.Errorf("failed to insert into index %s: %w", name, err)
		}
		updatedIndexes[name] = idx
		if r.logEnabled {
			r.logger.InfoContext(ctx, "Successfully inserted into index", "indexName", name, r.entityAttrs(obj))
		}
	}
	if r.logEnabled {
		r.logger.InfoContext(ctx, "Successfully inserted into all indexes", r.entityAttrs(obj))
	}
	return nil
}

func (r *RepoImpl[T]) Create(ctx context.Context, obj T) (uuid.UUID, error) {
	base := obj.GetBase()
	base.ID = uuid.New()
	base.CreatedAt = time.Now()
	base.UpdatedAt = base.CreatedAt

	opLogger := r.logger.With("op", "create", r.entityAttrs(obj))
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Creating new entity")
	}

	r.store.Store(base.ID, obj)
	if err := r.insertIndexes(ctx, obj); err != nil {
		r.store.Delete(base.ID) // Rollback main store
		if r.logEnabled {
			opLogger.ErrorContext(ctx, "Create failed, main store rolled back", "error", err)
		}
		return uuid.Nil, err
	}

	r.notify("create", obj)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Entity created successfully")
	}
	return base.ID, nil
}

func (r *RepoImpl[T]) Update(ctx context.Context, obj T) error {
	base := obj.GetBase()
	opLogger := r.logger.With("op", "update", "id", base.ID)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Updating entity")
	}
	existingObj, err := r.Get(ctx, base.ID)
	if err != nil {
		if r.logEnabled {
			opLogger.ErrorContext(ctx, "Update failed, entity not found", "error", err)
		}
		return err
	}

	r.store.Store(base.ID, obj)
	base.UpdatedAt = time.Now()
	oldObj := existingObj

	if err := r.insertIndexes(ctx, obj); err != nil {
		r.store.Store(base.ID, oldObj) // Rollback main store
		if r.logEnabled {
			opLogger.ErrorContext(ctx, "Update failed, main store rolled back", "error", err)
		}
		return err
	}

	r.notify("update", obj)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Entity updated successfully", "updatedName", obj.EntityName())
	}
	return nil
}

func (r *RepoImpl[T]) Get(ctx context.Context, id uuid.UUID) (T, error) {
	opLogger := r.logger.With("op", "get", "id", id)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Attempting to get entity")
	}
	v, ok := r.store.Load(id)
	if !ok {
		var zero T
		if r.logEnabled {
			opLogger.WarnContext(ctx, "Get failed, entity not found")
		}
		return zero, ErrNotFound
	}
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Successfully retrieved entity", "name", v.(T).EntityName())
	}
	return v.(T), nil
}

func (r *RepoImpl[T]) GetAllByIds(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]T, error) {
	opLogger := r.logger.With("op", "getAllByIds", "count", len(ids))
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Getting multiple entities by IDs")
	}
	result := make(map[uuid.UUID]T, len(ids))
	for _, id := range ids {
		if v, ok := r.store.Load(id); ok {
			result[id] = v.(T)
		}
	}
	if len(result) == 0 {
		if r.logEnabled {
			opLogger.WarnContext(ctx, "GetAllByIds failed, no entities found")
		}
		return nil, ErrNotFound
	}
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Successfully retrieved entities", "retrievedCount", len(result))
	}
	return result, nil
}

func (r *RepoImpl[T]) Delete(ctx context.Context, id uuid.UUID) error {
	opLogger := r.logger.With("op", "delete", "id", id)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Attempting to delete entity")
	}
	v, ok := r.store.Load(id)
	if !ok {
		if r.logEnabled {
			opLogger.WarnContext(ctx, "Delete failed, entity not found")
		}
		return ErrNotFound
	}
	obj := v.(T)
	r.store.Delete(id)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Deleted entity from main store", "name", obj.EntityName())
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	for name, idx := range r.indexes {
		idx.Delete(ctx, obj)
		if r.logEnabled {
			opLogger.InfoContext(ctx, "Deleted entity from index", "indexName", name, "name", obj.EntityName())
		}
	}

	r.notify("delete", obj)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Entity deleted successfully", "name", obj.EntityName())
	}
	return nil
}

func (r *RepoImpl[T]) GetAll(ctx context.Context) []T {
	opLogger := r.logger.With("op", "getAll")
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Getting all entities")
	}
	var result []T
	r.store.Range(func(_, value any) bool {
		result = append(result, value.(T))
		return true
	})
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Successfully retrieved all entities", "count", len(result))
	}
	return result
}

func (r *RepoImpl[T]) WithStore(fn func(store *sync.Map)) {
	if r.logEnabled {
		r.logger.Info("Executing WithStore callback")
	}
	fn(&r.store)
}

// Index management
func (r *RepoImpl[T]) AddIndex(ctx context.Context, name string, idx Indexer[T]) {
	opLogger := r.logger.With("op", "addIndex", "indexName", name)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Adding new index")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexes[name] = idx

	if r.logEnabled {
		opLogger.InfoContext(ctx, "Building index from existing data")
	}
	r.store.Range(func(_, value any) bool {
		if err := idx.Insert(ctx, value.(T)); err != nil {
			if r.logEnabled {
				opLogger.ErrorContext(ctx, "Failed to insert into index during build", "entityName", value.(T).EntityName(), "error", err)
			}
			return false
		}
		return true
	})
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Index built successfully")
	}
}

func (r *RepoImpl[T]) GetIndex(ctx context.Context, name string) (Indexer[T], bool) {
	opLogger := r.logger.With("op", "getIndex", "indexName", name)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Getting index by name")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx, ok := r.indexes[name]
	if !ok {
		if r.logEnabled {
			opLogger.WarnContext(ctx, "Index not found")
		}
	}
	return idx, ok
}

// AddSubscriber registers a channel to receive events.
func (r *RepoImpl[T]) AddSubscriber(ch chan Event[T]) {
	if r.logEnabled {
		r.logger.Info("Adding new subscriber")
	}
	r.subscribersMu.Lock()
	defer r.subscribersMu.Unlock()
	r.subscribers = append(r.subscribers, ch)
}

func (r *RepoImpl[T]) CompareAndSwap(ctx context.Context, key uuid.UUID, old T, new T) (bool, error) {
	opLogger := r.logger.With("op", "cas", "id", key)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Attempting CompareAndSwap", "oldName", old.EntityName(), "newName", new.EntityName())
	}
	swapped := r.store.CompareAndSwap(key, old, new)

	if !swapped {
		if r.logEnabled {
			opLogger.WarnContext(ctx, "CompareAndSwap failed due to mismatch")
		}
		return false, nil
	}
	if r.logEnabled {
		opLogger.InfoContext(ctx, "CompareAndSwap successful on main store")
	}

	if err := r.updateIndexesForCas(ctx, old, new); err != nil {
		r.store.Store(key, old)
		if r.logEnabled {
			opLogger.ErrorContext(ctx, "CAS index update failed, rolled back main store", "error", err)
		}
		return false, err
	}

	r.notify("cas", new)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "CompareAndSwap completed successfully", "finalName", new.EntityName())
	}
	return true, nil
}

func (r *RepoImpl[T]) updateIndexesForCas(ctx context.Context, old T, new T) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	opLogger := r.logger.With("op", "cas-index-update", "id", new.GetBase().ID)
	if r.logEnabled {
		opLogger.InfoContext(ctx, "Updating indexes for CAS operation")
	}
	updatedIndexes := make(map[string]Indexer[T])

	for name, idx := range r.indexes {
		if r.logEnabled {
			opLogger.InfoContext(ctx, "Deleting old object from index", "indexName", name)
		}
		idx.Delete(ctx, old)

		if err := idx.Insert(ctx, new); err != nil {
			if r.logEnabled {
				opLogger.ErrorContext(ctx, "Failed to insert new object, starting index rollback", "indexName", name, "error", err)
			}
			for _, rollbackIdx := range updatedIndexes {
				rollbackIdx.Insert(ctx, old)
			}
			idx.Insert(ctx, old)
			if r.logEnabled {
				opLogger.InfoContext(ctx, "Index rollback for CAS complete")
			}
			return fmt.Errorf("failed to update index %s during CAS: %w", name, err)
		}
		updatedIndexes[name] = idx
		if r.logEnabled {
			opLogger.InfoContext(ctx, "Successfully updated index", "indexName", name)
		}
	}

	if r.logEnabled {
		opLogger.InfoContext(ctx, "All indexes updated successfully for CAS operation")
	}
	return nil
}

// String implements fmt.Stringer
func (r *RepoImpl[T]) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	first := true
	r.store.Range(func(_, value any) bool {
		if !first {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%v", value))
		first = false
		return true
	})
	sb.WriteString("]")
	return sb.String()
}
