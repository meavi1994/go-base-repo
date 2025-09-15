// Package base_repo file: repo_impl.go

package base_repo

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"sync"
	"time"
)

// RepoImpl is a generic, thread-safe, in-memory repository.
type RepoImpl[T Entity] struct {
	store       sync.Map
	indexes     map[string]Indexer[T]
	subscribers []chan Event[T]
	mu          sync.RWMutex

	// WaitGroup for tracking goroutines and a mutex to protect its Add/Done calls
	// since wg.Add() can't be called concurrently with wg.Wait().
	subscribersMu sync.RWMutex // Protects the subscribers slice
}

func NewRepo[T Entity]() *RepoImpl[T] {
	return &RepoImpl[T]{
		indexes: make(map[string]Indexer[T]),
	}
}

// notify handles fan-out to subscribers.
func (r *RepoImpl[T]) notify(eventType string, obj T) {
	r.subscribersMu.RLock()
	defer r.subscribersMu.RUnlock()

	for _, ch := range r.subscribers {
		ev := Event[T]{Type: eventType, Obj: obj}
		ch <- ev
	}
}

func (r *RepoImpl[T]) rollback(ctx context.Context, updatedIndexes map[string]Indexer[T], obj T) {
	for _, rollbackIdx := range updatedIndexes {
		rollbackIdx.Delete(ctx, obj)
	}
}

func (r *RepoImpl[T]) insertIndexes(ctx context.Context, obj T) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	updatedIndexes := make(map[string]Indexer[T])

	for name, idx := range r.indexes {
		if err := idx.Insert(ctx, obj); err != nil {
			// Rollback on failure
			r.rollback(ctx, updatedIndexes, obj)
			return fmt.Errorf("failed to insert into index %s: %w", name, err)
		}
		updatedIndexes[name] = idx
	}
	return nil
}

func (r *RepoImpl[T]) Create(ctx context.Context, obj T) (uuid.UUID, error) {
	base := obj.GetBase()
	base.ID = uuid.New()
	base.CreatedAt = time.Now()
	base.UpdatedAt = base.CreatedAt

	r.store.Store(base.ID, obj)

	if err := r.insertIndexes(ctx, obj); err != nil {
		r.store.Delete(base.ID) // Rollback main store
		return uuid.Nil, err
	}

	r.notify("create", obj)
	return base.ID, nil
}

func (r *RepoImpl[T]) Update(ctx context.Context, obj T) error {
	base := obj.GetBase()
	existingObj, err := r.Get(ctx, base.ID)
	if err != nil {
		return err
	}

	r.store.Store(base.ID, obj)
	base.UpdatedAt = time.Now()

	// Capture the old object's state for potential rollback
	oldObj := existingObj

	// Attempt to update all indexes. Rollback is handled inside the helper.
	if err := r.insertIndexes(ctx, obj); err != nil {
		r.store.Store(base.ID, oldObj) // Rollback main store
		return err
	}

	r.notify("update", obj)
	return nil
}

func (r *RepoImpl[T]) Get(ctx context.Context, id uuid.UUID) (T, error) {
	v, ok := r.store.Load(id)
	if !ok {
		var zero T
		return zero, ErrNotFound
	}
	return v.(T), nil
}

func (r *RepoImpl[T]) GetAllByIds(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]T, error) {
	result := make(map[uuid.UUID]T, len(ids))
	for _, id := range ids {
		if v, ok := r.store.Load(id); ok {
			result[id] = v.(T)
		}
	}

	if len(result) == 0 {
		return nil, ErrNotFound
	}
	return result, nil
}

func (r *RepoImpl[T]) Delete(ctx context.Context, id uuid.UUID) error {
	v, ok := r.store.Load(id)
	if !ok {
		return ErrNotFound
	}
	obj := v.(T)
	r.store.Delete(id)

	r.mu.RLock()
	for _, idx := range r.indexes {
		idx.Delete(ctx, obj)
	}
	r.mu.RUnlock()

	r.notify("delete", obj)
	return nil
}

func (r *RepoImpl[T]) GetAll(ctx context.Context) []T {
	var result []T
	r.store.Range(func(_, value any) bool {
		result = append(result, value.(T))
		return true
	})
	return result
}

func (r *RepoImpl[T]) WithStore(fn func(store *sync.Map)) {
	fn(&r.store)
}

// Index management
func (r *RepoImpl[T]) AddIndex(ctx context.Context, name string, idx Indexer[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexes[name] = idx
	// Build index from existing data
	r.store.Range(func(_, value any) bool {
		idx.Insert(ctx, value.(T))
		return true
	})
}

func (r *RepoImpl[T]) GetIndex(ctx context.Context, name string) (Indexer[T], bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx, ok := r.indexes[name]
	return idx, ok
}

// AddSubscriber registers a channel to receive events.
func (r *RepoImpl[T]) AddSubscriber(ch chan Event[T]) {
	r.subscribersMu.Lock()
	defer r.subscribersMu.Unlock()
	r.subscribers = append(r.subscribers, ch)
}

func (r *RepoImpl[T]) CompareAndSwap(ctx context.Context, key uuid.UUID, old T, new T) (bool, error) {
	// The sync.Map.CompareAndSwap method requires the values to be of the same type as stored.
	swapped := r.store.CompareAndSwap(key, old, new)

	if !swapped {
		// The value in the map was not 'old', so the operation failed.
		return false, nil
	}

	// The swap in the main store succeeded. Now, update the indexes.
	// We'll use the same pattern as in the `Create` and `Update` methods.
	if err := r.updateIndexesForCas(ctx, old, new); err != nil {
		// Rollback the main store change.
		r.store.Store(key, old)
		return false, err
	}

	r.notify("cas", new)
	return true, nil
}
func (r *RepoImpl[T]) updateIndexesForCas(ctx context.Context, old T, new T) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	updatedIndexes := make(map[string]Indexer[T])

	for name, idx := range r.indexes {
		// A potential issue: if the index key for 'old' is the same as for 'new',
		// calling Delete then Insert is inefficient. The Insert method should handle this.
		// For a non-unique index, Delete removes a specific item from a collection.
		// For a unique index, Insert handles the replacement.

		idx.Delete(ctx, old) // Delete the old object from the index.

		if err := idx.Insert(ctx, new); err != nil {
			// Rollback: Re-insert the old object into indexes that were already updated
			// or where the Delete was successful.
			for _, rollbackIdx := range updatedIndexes {
				rollbackIdx.Insert(ctx, old)
			}

			// We need to re-insert the `old` object into the index where the `new`
			// object failed to insert.
			idx.Insert(ctx, old)

			return fmt.Errorf("failed to update index %s during CAS: %w", name, err)
		}
		updatedIndexes[name] = idx
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
