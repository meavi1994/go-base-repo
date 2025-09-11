package base_repo

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"sync"
	"time"
)

var ErrNotFound = errors.New("not found")

// BaseModel provides common fields for all entities.
type BaseModel struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Entity interface for any object stored in Repo.
type Entity interface {
	GetBase() *BaseModel
}

// Repo is a generic, thread-safe, in-memory repository.
type Repo[T Entity] struct {
	store sync.Map // map[uuid.UUID]T
}

// NewRepo creates a new empty repository.
func NewRepo[T Entity]() *Repo[T] {
	return &Repo[T]{}
}

// Create inserts a new object, assigns UUID, and timestamps.
func (r *Repo[T]) Create(ctx context.Context, obj T) uuid.UUID {
	base := obj.GetBase()
	base.ID = uuid.New()
	base.CreatedAt = time.Now()
	base.UpdatedAt = base.CreatedAt

	r.store.Store(base.ID, obj)
	return base.ID
}

// Get retrieves an object by UUID.
func (r *Repo[T]) Get(ctx context.Context, id uuid.UUID) (T, error) {
	v, ok := r.store.Load(id)
	if !ok {
		var zero T
		return zero, ErrNotFound
	}
	return v.(T), nil
}

// GetAll returns a snapshot of all objects.
func (r *Repo[T]) GetAll(ctx context.Context) []T {
	var result []T
	r.store.Range(func(_, value any) bool {
		result = append(result, value.(T))
		return true
	})
	return result
}

// Update replaces an object and updates the UpdatedAt timestamp.
func (r *Repo[T]) Update(ctx context.Context, obj T) error {
	base := obj.GetBase()
	base.UpdatedAt = time.Now()
	r.store.Store(base.ID, obj)
	return nil
}

// Delete removes an object by UUID.
func (r *Repo[T]) Delete(ctx context.Context, id uuid.UUID) error {
	r.store.Delete(id)
	return nil
}

// Find returns all objects matching the predicate.
func (r *Repo[T]) Find(ctx context.Context, predicate func(T) bool) []T {
	var result []T
	r.store.Range(func(_, value any) bool {
		obj := value.(T)
		if predicate(obj) {
			result = append(result, obj)
		}
		return true
	})
	return result
}

// FindFirst returns the first object matching the predicate.
func (r *Repo[T]) FindFirst(ctx context.Context, predicate func(T) bool) (T, bool) {
	var zero T
	found := false
	r.store.Range(func(_, value any) bool {
		obj := value.(T)
		if predicate(obj) {
			zero = obj
			found = true
			return false
		}
		return true
	})
	return zero, found
}

// Count returns the number of objects optionally matching a predicate.
func (r *Repo[T]) Count(ctx context.Context, predicate func(T) bool) int {
	count := 0
	r.store.Range(func(_, value any) bool {
		obj := value.(T)
		if predicate == nil || predicate(obj) {
			count++
		}
		return true
	})
	return count
}

// Exists checks if an object exists by UUID.
func (r *Repo[T]) Exists(ctx context.Context, id uuid.UUID) bool {
	_, ok := r.store.Load(id)
	return ok
}

// WithStore allows direct access to the underlying sync.Map for custom operations.
// Example: bulk updates or advanced queries.
func (r *Repo[T]) WithStore(fn func(store *sync.Map)) {
	fn(&r.store)
}
