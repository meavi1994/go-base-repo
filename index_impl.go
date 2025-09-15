// Package base_repo file: index_impl.go

package base_repo

import (
	"fmt"
	"github.com/google/btree"
	"github.com/google/uuid"
	"strings"
	"sync"
)

// BTreeIndex implements Index[T] using a B-Tree.
// It stores a collection of entities for each key to handle duplicates.
type BTreeIndex[T Entity] struct {
	tree    *btree.BTreeG[item[T]]
	keyFunc func(T) string
	unique  bool
}

// NewIndex creates a new BTree-based Index with a given key extractor.
// It uses btree.NewG and a custom comparison function.
// The degree is fixed at 2 for this implementation.
func newIndex[T Entity](keyFunc func(T) string, unique bool) *BTreeIndex[T] {
	lessFunc := func(a, b item[T]) bool {
		return a.key < b.key
	}
	return &BTreeIndex[T]{
		tree:    btree.NewG(2, lessFunc),
		keyFunc: keyFunc,
		unique:  unique,
	}
}

func NewIndex[T Entity](keyFunc func(T) string) *BTreeIndex[T] {
	return newIndex(keyFunc, false)
}

func NewUniqueIndex[T Entity](keyFunc func(T) string) *BTreeIndex[T] {
	return newIndex(keyFunc, true)
}

// Insert or replace entity in index.
func (idx *BTreeIndex[T]) Insert(obj T) error {
	if idx.unique {
		return idx.insertUnique(obj)
	}
	idx.insertNonUnique(obj)
	return nil
}

func (idx *BTreeIndex[T]) insertNonUnique(obj T) {
	key := idx.keyFunc(obj)

	// Get the existing item and the 'found' boolean
	it, found := idx.tree.Get(item[T]{key: key})

	if found {
		// Key exists, add to the existing collection
		collection := it.value
		collection.Store(obj.GetBase().ID, obj)
	} else {
		// Key does not exist, create a new collection
		newCollection := &sync.Map{}
		newCollection.Store(obj.GetBase().ID, obj)
		idx.tree.ReplaceOrInsert(item[T]{key: key, value: newCollection})
	}
}

func (idx *BTreeIndex[T]) insertUnique(obj T) error {
	key := idx.keyFunc(obj)

	// Get the existing item and the 'found' boolean
	it, found := idx.tree.Get(item[T]{key: key})

	if found {
		// Key exists, now check if the IDs are the same
		existingMap := it.value

		// Use a variable to store the existing ID found in the map
		var existingID uuid.UUID

		// Range over the map to find the existing object's ID
		existingMap.Range(func(id, _ any) bool {
			existingID = id.(uuid.UUID)
			return false // Stop after the first (and only) item
		})

		// If the IDs don't match, it's a duplicate
		if existingID != obj.GetBase().ID {
			return ErrDuplicateKey
		}
	}

	// It's a new item or an update to an existing one, so proceed.
	// Create a new map with a single item to replace the old one.
	newMap := &sync.Map{}
	newMap.Store(obj.GetBase().ID, obj)

	// Use ReplaceOrInsert to handle both new items and updates
	idx.tree.ReplaceOrInsert(item[T]{key: key, value: newMap})

	return nil
}

// Delete entity from index.
func (idx *BTreeIndex[T]) Delete(obj T) {
	key := idx.keyFunc(obj)

	existingItem, found := idx.tree.Get(item[T]{key: key})
	if found {
		collection := existingItem.value
		collection.Delete(obj.GetBase().ID)

		isEmpty := true
		collection.Range(func(_, _ any) bool {
			isEmpty = false
			return false
		})

		if isEmpty {
			idx.tree.Delete(existingItem)
		}
	}
}

// Find returns all entities for a given key.
func (idx *BTreeIndex[T]) Find(key string) []T {
	it, found := idx.tree.Get(item[T]{key: key})
	if !found {
		return nil
	}

	var results []T
	collection := it.value
	collection.Range(func(_, value any) bool {
		results = append(results, value.(T))
		return true
	})
	return results
}

// forRange is a generic iterator helper to avoid code duplication.
// It accepts a btree iteration function and a user-provided callback.
func (idx *BTreeIndex[T]) forRange(
	btreeIterFn func(fn func(i item[T]) bool),
	userFn func(T) bool,
) {
	btreeIterFn(func(i item[T]) bool {
		var continueIteration = true
		i.value.Range(func(_, value any) bool {
			if !userFn(value.(T)) {
				continueIteration = false
				return false
			}
			return true
		})
		return continueIteration
	})
}

// Ascend iterates in ascending order.
func (idx *BTreeIndex[T]) Ascend(fn func(T) bool) {
	idx.forRange(func(btreeFn func(i item[T]) bool) {
		idx.tree.Ascend(btreeFn)
	}, fn)
}

// Descend iterates in descending order.
func (idx *BTreeIndex[T]) Descend(fn func(T) bool) {
	idx.forRange(
		func(btreeFn func(i item[T]) bool) {
			idx.tree.Descend(btreeFn)
		},
		fn)
}

// AscendRange iterates between [lower, upper].
func (idx *BTreeIndex[T]) AscendRange(lower, upper string, fn func(T) bool) {
	idx.forRange(
		func(btreeFn func(i item[T]) bool) {
			idx.tree.AscendRange(item[T]{key: lower}, item[T]{key: upper}, btreeFn)
		},
		fn,
	)
}

// DescendRange iterates between [upper, lower] in reverse.
func (idx *BTreeIndex[T]) DescendRange(lower, upper string, fn func(T) bool) {
	idx.forRange(
		func(btreeFn func(i item[T]) bool) {
			idx.tree.DescendRange(item[T]{key: upper}, item[T]{key: lower}, btreeFn)
		},
		fn,
	)
}

// AscendGreaterThanOrEqual iterates from key to the end in ascending order.
func (idx *BTreeIndex[T]) AscendGreaterThanOrEqual(key string, fn func(T) bool) {
	idx.forRange(
		func(btreeFn func(i item[T]) bool) {
			idx.tree.AscendGreaterOrEqual(item[T]{key: key}, btreeFn)
		},
		fn,
	)
}

// DescendLessThanOrEqual iterates from key to the beginning in descending order.
func (idx *BTreeIndex[T]) DescendLessThanOrEqual(key string, fn func(T) bool) {
	idx.forRange(
		func(btreeFn func(i item[T]) bool) {
			idx.tree.DescendLessOrEqual(item[T]{key: key}, btreeFn)
		},
		fn,
	)
}

func (idx *BTreeIndex[T]) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	first := true

	idx.Ascend(func(obj T) bool {
		if !first {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%v", obj))
		first = false
		return true // Continue iteration
	})

	sb.WriteString("]")
	return sb.String()
}
