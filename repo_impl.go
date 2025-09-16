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
}

func NewRepo[T Entity]() *RepoImpl[T] {
	return &RepoImpl[T]{
		indexes:    make(map[string]Indexer[T]),
		logEnabled: false,
	}
}

// NewRepoWithLogs enables logging for the repository.
func NewRepoWithLogs[T Entity]() *RepoImpl[T] {
	return &RepoImpl[T]{
		indexes:    make(map[string]Indexer[T]),
		logEnabled: true,
	}
}

// The following methods have been updated with slog.Default().
// We'll use a helper method `logInfo` to simplify logging calls.
func (r *RepoImpl[T]) logInfo(ctx context.Context, msg string, args ...any) {
	if r.logEnabled {
		slog.Default().InfoContext(ctx, msg, args...)
	}
}

func (r *RepoImpl[T]) notify(eventType string, obj T) {
	r.logInfo(context.Background(), "Notifying subscribers", "eventType", eventType, "id", obj.GetBase().ID)
	r.subscribersMu.RLock()
	defer r.subscribersMu.RUnlock()

	for _, ch := range r.subscribers {
		ev := Event[T]{Type: eventType, Obj: obj}
		ch <- ev
	}
}

func (r *RepoImpl[T]) rollback(ctx context.Context, updatedIndexes map[string]Indexer[T], obj T) {
	r.logInfo(ctx, "Starting index rollback", "id", obj.GetBase().ID)
	for name, rollbackIdx := range updatedIndexes {
		rollbackIdx.Delete(ctx, obj)
		r.logInfo(ctx, "Rolled back index", "name", name, "id", obj.GetBase().ID)
	}
}

func (r *RepoImpl[T]) insertIndexes(ctx context.Context, obj T) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logInfo(ctx, "Attempting to insert into indexes", "id", obj.GetBase().ID, "numIndexes", len(r.indexes))
	updatedIndexes := make(map[string]Indexer[T])

	for name, idx := range r.indexes {
		if err := idx.Insert(ctx, obj); err != nil {
			r.logInfo(ctx, "Failed to insert into index, beginning rollback", "name", name, "id", obj.GetBase().ID, "error", err)
			r.rollback(ctx, updatedIndexes, obj)
			return fmt.Errorf("failed to insert into index %s: %w", name, err)
		}
		updatedIndexes[name] = idx
		r.logInfo(ctx, "Successfully inserted into index", "name", name, "id", obj.GetBase().ID)
	}
	r.logInfo(ctx, "Successfully inserted into all indexes", "id", obj.GetBase().ID)
	return nil
}

func (r *RepoImpl[T]) Create(ctx context.Context, obj T) (uuid.UUID, error) {
	base := obj.GetBase()
	base.ID = uuid.New()
	base.CreatedAt = time.Now()
	base.UpdatedAt = base.CreatedAt
	r.logInfo(ctx, "Creating new entity", "id", base.ID)

	r.store.Store(base.ID, obj)

	if err := r.insertIndexes(ctx, obj); err != nil {
		r.store.Delete(base.ID) // Rollback main store
		r.logInfo(ctx, "Create failed, main store rolled back", "id", base.ID, "error", err)
		return uuid.Nil, err
	}

	r.notify("create", obj)
	r.logInfo(ctx, "Entity created successfully", "id", base.ID)
	return base.ID, nil
}

func (r *RepoImpl[T]) Update(ctx context.Context, obj T) error {
	base := obj.GetBase()
	r.logInfo(ctx, "Updating entity", "id", base.ID)
	existingObj, err := r.Get(ctx, base.ID)
	if err != nil {
		r.logInfo(ctx, "Update failed, entity not found", "id", base.ID, "error", err)
		return err
	}

	r.store.Store(base.ID, obj)
	base.UpdatedAt = time.Now()

	oldObj := existingObj

	if err := r.insertIndexes(ctx, obj); err != nil {
		r.store.Store(base.ID, oldObj) // Rollback main store
		r.logInfo(ctx, "Update failed, main store rolled back", "id", base.ID, "error", err)
		return err
	}

	r.notify("update", obj)
	r.logInfo(ctx, "Entity updated successfully", "id", base.ID)
	return nil
}

func (r *RepoImpl[T]) Get(ctx context.Context, id uuid.UUID) (T, error) {
	r.logInfo(ctx, "Attempting to get entity", "id", id)
	v, ok := r.store.Load(id)
	if !ok {
		var zero T
		r.logInfo(ctx, "Get failed, entity not found", "id", id)
		return zero, ErrNotFound
	}
	r.logInfo(ctx, "Successfully retrieved entity", "id", id)
	return v.(T), nil
}

func (r *RepoImpl[T]) GetAllByIds(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]T, error) {
	r.logInfo(ctx, "Getting multiple entities by IDs", "count", len(ids))
	result := make(map[uuid.UUID]T, len(ids))
	for _, id := range ids {
		if v, ok := r.store.Load(id); ok {
			result[id] = v.(T)
		}
	}

	if len(result) == 0 {
		r.logInfo(ctx, "GetAllByIds failed, no entities found")
		return nil, ErrNotFound
	}
	r.logInfo(ctx, "Successfully retrieved entities", "count", len(result))
	return result, nil
}

func (r *RepoImpl[T]) Delete(ctx context.Context, id uuid.UUID) error {
	r.logInfo(ctx, "Attempting to delete entity", "id", id)
	v, ok := r.store.Load(id)
	if !ok {
		r.logInfo(ctx, "Delete failed, entity not found", "id", id)
		return ErrNotFound
	}
	obj := v.(T)
	r.store.Delete(id)
	r.logInfo(ctx, "Deleted entity from main store", "id", id)

	r.mu.RLock()
	for name, idx := range r.indexes {
		idx.Delete(ctx, obj)
		r.logInfo(ctx, "Deleted entity from index", "name", name, "id", id)
	}
	r.mu.RUnlock()

	r.notify("delete", obj)
	r.logInfo(ctx, "Entity deleted successfully", "id", id)
	return nil
}

func (r *RepoImpl[T]) GetAll(ctx context.Context) []T {
	r.logInfo(ctx, "Getting all entities")
	var result []T
	r.store.Range(func(_, value any) bool {
		result = append(result, value.(T))
		return true
	})
	r.logInfo(ctx, "Successfully retrieved all entities", "count", len(result))
	return result
}

func (r *RepoImpl[T]) WithStore(fn func(store *sync.Map)) {
	r.logInfo(context.Background(), "Executing WithStore callback")
	fn(&r.store)
}

// Index management
func (r *RepoImpl[T]) AddIndex(ctx context.Context, name string, idx Indexer[T]) {
	r.logInfo(ctx, "Adding new index", "name", name)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexes[name] = idx
	r.logInfo(ctx, "Building index from existing data", "name", name)
	r.store.Range(func(_, value any) bool {
		if err := idx.Insert(ctx, value.(T)); err != nil {
			r.logInfo(ctx, "Failed to insert into index during build", "name", name, "error", err)
			return false
		}
		return true
	})
	r.logInfo(ctx, "Index built successfully", "name", name)
}

func (r *RepoImpl[T]) GetIndex(ctx context.Context, name string) (Indexer[T], bool) {
	r.logInfo(ctx, "Getting index by name", "name", name)
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx, ok := r.indexes[name]
	if !ok {
		r.logInfo(ctx, "Index not found", "name", name)
	}
	return idx, ok
}

// AddSubscriber registers a channel to receive events.
func (r *RepoImpl[T]) AddSubscriber(ch chan Event[T]) {
	r.logInfo(context.Background(), "Adding new subscriber")
	r.subscribersMu.Lock()
	defer r.subscribersMu.Unlock()
	r.subscribers = append(r.subscribers, ch)
}

func (r *RepoImpl[T]) CompareAndSwap(ctx context.Context, key uuid.UUID, old T, new T) (bool, error) {
	r.logInfo(ctx, "Attempting CompareAndSwap", "id", key)
	swapped := r.store.CompareAndSwap(key, old, new)

	if !swapped {
		r.logInfo(ctx, "CompareAndSwap failed due to mismatch", "id", key)
		return false, nil
	}
	r.logInfo(ctx, "CompareAndSwap successful on main store", "id", key)

	if err := r.updateIndexesForCas(ctx, old, new); err != nil {
		r.store.Store(key, old)
		r.logInfo(ctx, "CAS index update failed, rolled back main store", "id", key, "error", err)
		return false, err
	}

	r.notify("cas", new)
	r.logInfo(ctx, "CompareAndSwap completed successfully", "id", key)
	return true, nil
}
func (r *RepoImpl[T]) updateIndexesForCas(ctx context.Context, old T, new T) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logInfo(ctx, "Updating indexes for CAS operation", "id", new.GetBase().ID)
	updatedIndexes := make(map[string]Indexer[T])

	for name, idx := range r.indexes {
		// Use slog's structured logging to provide context for each index operation
		indexLogger := slog.Default().With("indexName", name, "id", new.GetBase().ID)

		indexLogger.Info("Deleting old object from index")
		idx.Delete(ctx, old)

		if err := idx.Insert(ctx, new); err != nil {
			indexLogger.Error("Failed to insert new object, starting index rollback", "error", err)

			// Rollback: Re-insert the old object into indexes that were already updated
			for _, rollbackIdx := range updatedIndexes {
				rollbackIdx.Insert(ctx, old)
			}

			// Re-insert the old object into the index where the new one failed
			idx.Insert(ctx, old)
			indexLogger.Info("Index rollback for CAS complete")
			return fmt.Errorf("failed to update index %s during CAS: %w", name, err)
		}
		updatedIndexes[name] = idx
		indexLogger.Info("Successfully updated index")
	}

	r.logInfo(ctx, "All indexes updated successfully for CAS operation", "id", new.GetBase().ID)
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
