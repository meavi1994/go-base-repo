// Package base_repo file: index_impl.go

package base_repo

import (
	"context"
	"fmt"
	"github.com/google/btree"
	"github.com/google/uuid"
	"strings"
	"sync"
)

// BTreeIndex now has a type parameter for the key.
// K represents the key type, T is the entity type.
type BTreeIndex[K comparable, T Entity] struct {
	tree    *btree.BTreeG[item[K, T]]
	keyFunc func(T) K
	unique  bool
}

// NewIndex creates a new BTree-based Index with a generic key extractor.
// It requires a lessFunc to compare the keys.
func newIndex[K comparable, T Entity](
	keyFunc func(T) K,
	lessFunc func(a, b K) bool,
	unique bool,
) *BTreeIndex[K, T] {
	// The lessFunc for the btree now uses the user-provided lessFunc for keys.
	btreeLessFunc := func(a, b item[K, T]) bool {
		return lessFunc(a.key, b.key)
	}
	return &BTreeIndex[K, T]{
		tree:    btree.NewG(2, btreeLessFunc),
		keyFunc: keyFunc,
		unique:  unique,
	}
}

// A generic version of the constructor, requiring the lessFunc.
func NewIndex[K comparable, T Entity](keyFunc func(T) K, lessFunc func(a, b K) bool) *BTreeIndex[K, T] {
	return newIndex(keyFunc, lessFunc, false)
}

func NewUniqueIndex[K comparable, T Entity](keyFunc func(T) K, lessFunc func(a, b K) bool) *BTreeIndex[K, T] {
	return newIndex(keyFunc, lessFunc, true)
}

// Insert or replace entity in index.
func (idx *BTreeIndex[K, T]) Insert(ctx context.Context, obj T) error {
	if idx.unique {
		return idx.insertUnique(obj)
	}
	idx.insertNonUnique(obj)
	return nil
}

func (idx *BTreeIndex[K, T]) insertNonUnique(obj T) {
	key := idx.keyFunc(obj)

	// Get the existing item and the 'found' boolean
	it, found := idx.tree.Get(item[K, T]{key: key})

	if found {
		// Key exists, add to the existing collection
		collection := it.value
		collection.Store(obj.GetBase().ID, obj)
	} else {
		// Key does not exist, create a new collection
		newCollection := &sync.Map{}
		newCollection.Store(obj.GetBase().ID, obj)
		idx.tree.ReplaceOrInsert(item[K, T]{key: key, value: newCollection})
	}
}

func (idx *BTreeIndex[K, T]) insertUnique(obj T) error {
	key := idx.keyFunc(obj)

	// Get the existing item and the 'found' boolean
	it, found := idx.tree.Get(item[K, T]{key: key})

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
	idx.tree.ReplaceOrInsert(item[K, T]{key: key, value: newMap})

	return nil
}

// Delete entity from index.
func (idx *BTreeIndex[K, T]) Delete(ctx context.Context, obj T) {
	key := idx.keyFunc(obj)

	existingItem, found := idx.tree.Get(item[K, T]{key: key})
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
func (idx *BTreeIndex[K, T]) Find(ctx context.Context, key K) []T {
	it, found := idx.tree.Get(item[K, T]{key: key})
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
func (idx *BTreeIndex[K, T]) forRange(
	ctx context.Context,
	btreeIterFn func(fn func(i item[K, T]) bool),
	userFn func(T) bool,
) {
	btreeIterFn(func(i item[K, T]) bool {
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
func (idx *BTreeIndex[K, T]) Ascend(ctx context.Context, fn func(T) bool) {
	idx.forRange(ctx, func(btreeFn func(i item[K, T]) bool) {
		idx.tree.Ascend(btreeFn)
	}, fn)
}

// Descend iterates in descending order.
func (idx *BTreeIndex[K, T]) Descend(ctx context.Context, fn func(T) bool) {
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.Descend(btreeFn)
		},
		fn)
}

// AscendRange iterates between [lower, upper].
func (idx *BTreeIndex[K, T]) AscendRange(ctx context.Context, lower, upper K, fn func(T) bool) {
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.AscendRange(item[K, T]{key: lower}, item[K, T]{key: upper}, btreeFn)
		},
		fn,
	)
}

// DescendRange iterates between [upper, lower] in reverse.
func (idx *BTreeIndex[K, T]) DescendRange(ctx context.Context, lower, upper K, fn func(T) bool) {
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.DescendRange(item[K, T]{key: upper}, item[K, T]{key: lower}, btreeFn)
		},
		fn,
	)
}

// AscendGreaterThanOrEqual iterates from key to the end in ascending order.
func (idx *BTreeIndex[K, T]) AscendGreaterThanOrEqual(ctx context.Context, key K, fn func(T) bool) {
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.AscendGreaterOrEqual(item[K, T]{key: key}, btreeFn)
		},
		fn,
	)
}

// DescendLessThanOrEqual iterates from key to the beginning in descending order.
func (idx *BTreeIndex[K, T]) DescendLessThanOrEqual(ctx context.Context, key K, fn func(T) bool) {
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.DescendLessOrEqual(item[K, T]{key: key}, btreeFn)
		},
		fn,
	)
}

func (idx *BTreeIndex[K, T]) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	first := true

	idx.Ascend(context.TODO(), func(obj T) bool {
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
