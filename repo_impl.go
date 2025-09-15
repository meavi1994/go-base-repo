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
	indexes     map[string]Index[T]
	subscribers []func(event string, obj T)
	mu          sync.RWMutex

	// WaitGroup for tracking goroutines and a mutex to protect its Add/Done calls
	// since wg.Add() can't be called concurrently with wg.Wait().
	subscribersMu sync.RWMutex // Protects the subscribers slice
	wg            sync.WaitGroup
	wgMu          sync.Mutex
}

func NewRepo[T Entity]() *RepoImpl[T] {
	return &RepoImpl[T]{
		indexes: make(map[string]Index[T]),
	}
}

// notify handles fan-out to subscribers.
func (r *RepoImpl[T]) notify(event string, obj T) {
	r.subscribersMu.RLock()
	defer r.subscribersMu.RUnlock()

	// Use a local variable to capture the number of subscribers to avoid a race
	// condition if the slice changes size.
	numSubscribers := len(r.subscribers)

	r.wgMu.Lock()
	r.wg.Add(numSubscribers)
	r.wgMu.Unlock()

	for _, sub := range r.subscribers {
		// Start a new goroutine for each subscriber.
		go func(s func(string, T)) {
			defer r.wg.Done() // 👈 wg.Done() is now here
			s(event, obj)
		}(sub)
	}
}

func (r *RepoImpl[T]) rollback(updatedIndexes map[string]Index[T], obj T) {
	for _, rollbackIdx := range updatedIndexes {
		rollbackIdx.Delete(obj)
	}
}

func (r *RepoImpl[T]) insertIndexes(obj T) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	updatedIndexes := make(map[string]Index[T])

	for name, idx := range r.indexes {
		if err := idx.Insert(obj); err != nil {
			// Rollback on failure
			r.rollback(updatedIndexes, obj)
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

	if err := r.insertIndexes(obj); err != nil {
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
	if err := r.insertIndexes(obj); err != nil {
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

func (r *RepoImpl[T]) Delete(ctx context.Context, id uuid.UUID) error {
	v, ok := r.store.Load(id)
	if !ok {
		return ErrNotFound
	}
	obj := v.(T)
	r.store.Delete(id)

	r.mu.RLock()
	for _, idx := range r.indexes {
		idx.Delete(obj)
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
func (r *RepoImpl[T]) AddIndex(name string, idx Index[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexes[name] = idx
	// Build index from existing data
	r.store.Range(func(_, value any) bool {
		idx.Insert(value.(T))
		return true
	})
}

func (r *RepoImpl[T]) GetIndex(name string) (Index[T], bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx, ok := r.indexes[name]
	return idx, ok
}

// Subscribers
func (r *RepoImpl[T]) AddSubscriber(fn func(event string, obj T)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscribers = append(r.subscribers, fn)
}

// WaitTillNotificationDone blocks until all pending notification goroutines are finished.
func (r *RepoImpl[T]) WaitTillNotificationDone() {
	r.wg.Wait()
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
