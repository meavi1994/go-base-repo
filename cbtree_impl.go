package base_repo

import (
	"github.com/google/btree"
	"sync"
)

type ItemIteratorG[T any] func(item T) bool

type BTreeG[T any] interface {
	// CRUD
	ReplaceOrInsert(item T) (T, bool) // Create/Update
	Get(key T) (T, bool)              // Read
	Delete(item T) (T, bool)          // Delete
	Len() int                         // Utility: number of items

	// Traversal
	Ascend(fn btree.ItemIteratorG[T])
	Descend(fn btree.ItemIteratorG[T])
	AscendGreaterOrEqual(pivot T, fn btree.ItemIteratorG[T])
	DescendLessOrEqual(pivot T, fn btree.ItemIteratorG[T])
	AscendRange(greaterOrEqual, lessThan T, fn btree.ItemIteratorG[T])
	DescendRange(lessOrEqual, greaterThan T, fn btree.ItemIteratorG[T])

	// Min/Max
	Min() (T, bool)
	Max() (T, bool)
}

type SafeBTreeG[T any] struct {
	mu sync.RWMutex
	bt *btree.BTreeG[T]
}

func NewSafeBTreeG[T any](degree int, less func(a, b T) bool) *SafeBTreeG[T] {
	return &SafeBTreeG[T]{bt: btree.NewG(degree, less)}
}

// --- CRUD ---
func (s *SafeBTreeG[T]) ReplaceOrInsert(item T) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bt.ReplaceOrInsert(item)
}

func (s *SafeBTreeG[T]) Get(key T) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bt.Get(key)
}

func (s *SafeBTreeG[T]) Delete(item T) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bt.Delete(item)
}

func (s *SafeBTreeG[T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bt.Len()
}

// --- Traversal ---
func (s *SafeBTreeG[T]) Ascend(fn btree.ItemIteratorG[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.bt.Ascend(fn)
}

func (s *SafeBTreeG[T]) Descend(fn btree.ItemIteratorG[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.bt.Descend(fn)
}

func (s *SafeBTreeG[T]) AscendGreaterOrEqual(pivot T, fn btree.ItemIteratorG[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.bt.AscendGreaterOrEqual(pivot, fn)
}

func (s *SafeBTreeG[T]) DescendLessOrEqual(pivot T, fn btree.ItemIteratorG[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.bt.DescendLessOrEqual(pivot, fn)
}

func (s *SafeBTreeG[T]) AscendRange(greaterOrEqual, lessThan T, fn btree.ItemIteratorG[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.bt.AscendRange(greaterOrEqual, lessThan, fn)
}

func (s *SafeBTreeG[T]) DescendRange(lessOrEqual, greaterThan T, fn btree.ItemIteratorG[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.bt.DescendRange(lessOrEqual, greaterThan, fn)
}

// --- Min/Max ---
func (s *SafeBTreeG[T]) Min() (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bt.Min()
}

func (s *SafeBTreeG[T]) Max() (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bt.Max()
}
