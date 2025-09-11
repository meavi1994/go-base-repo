package base_repo

import (
	"context"
	"github.com/google/uuid"
)

type MockRepo[T Entity] struct {
	Store map[uuid.UUID]T
}

func NewMockRepo[T Entity](store map[uuid.UUID]T) *MockRepo[T] {
	return &MockRepo[T]{Store: store}
}

func (m *MockRepo[T]) Create(ctx context.Context, obj T) uuid.UUID {
	base := obj.GetBase()
	if base.ID == uuid.Nil {
		base.ID = uuid.New()
	}
	m.Store[base.ID] = obj
	return base.ID
}

func (m *MockRepo[T]) Get(ctx context.Context, id uuid.UUID) (T, error) {
	if obj, ok := m.Store[id]; ok {
		return obj, nil
	}
	var zero T
	return zero, ErrNotFound
}

func (m *MockRepo[T]) GetAll(ctx context.Context) []T {
	var result []T
	for _, v := range m.Store {
		result = append(result, v)
	}
	return result
}

func (m *MockRepo[T]) GetAllByIds(ctx context.Context, ids []uuid.UUID) map[uuid.UUID]T {
	result := make(map[uuid.UUID]T)
	for _, id := range ids {
		if v, ok := m.Store[id]; ok {
			result[id] = v
		}
	}
	return result
}

// Stubbed methods (not needed in most tests, but present to satisfy interface)
func (m *MockRepo[T]) Update(ctx context.Context, obj T) error {
	m.Store[obj.GetBase().ID] = obj
	return nil
}
func (m *MockRepo[T]) Delete(ctx context.Context, id uuid.UUID) error {
	delete(m.Store, id)
	return nil
}
func (m *MockRepo[T]) Find(ctx context.Context, predicate func(T) bool) []T {
	var r []T
	for _, v := range m.Store {
		if predicate(v) {
			r = append(r, v)
		}
	}
	return r
}
func (m *MockRepo[T]) FindFirst(ctx context.Context, predicate func(T) bool) (T, bool) {
	for _, v := range m.Store {
		if predicate(v) {
			return v, true
		}
	}
	var zero T
	return zero, false
}
func (m *MockRepo[T]) Count(ctx context.Context, predicate func(T) bool) int {
	c := 0
	for _, v := range m.Store {
		if predicate == nil || predicate(v) {
			c++
		}
	}
	return c
}
func (m *MockRepo[T]) Exists(ctx context.Context, id uuid.UUID) bool { _, ok := m.Store[id]; return ok }
