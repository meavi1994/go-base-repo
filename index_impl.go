// Package base_repo file: index_impl.go

package base_repo

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/google/btree"
	"github.com/google/uuid"
)

// BTreeIndex now has a type parameter for the key.
// K represents the key type, T is the entity type.
type BTreeIndex[K comparable, T Entity] struct {
	tree    *btree.BTreeG[item[K, T]]
	keyFunc func(T) K
	unique  bool
	// A field to control logging behavior
	logEnabled bool
}

// Private constructor
func newIndex[K comparable, T Entity](
	keyFunc func(T) K,
	lessFunc func(a, b K) bool,
	unique bool,
	logEnabled bool,
) *BTreeIndex[K, T] {
	// The lessFunc for the btree now uses the user-provided lessFunc for keys.
	btreeLessFunc := func(a, b item[K, T]) bool {
		return lessFunc(a.key, b.key)
	}
	return &BTreeIndex[K, T]{
		tree:       btree.NewG(2, btreeLessFunc),
		keyFunc:    keyFunc,
		unique:     unique,
		logEnabled: logEnabled,
	}
}

// A generic version of the constructor, requiring the lessFunc.
func NewIndex[K comparable, T Entity](keyFunc func(T) K, lessFunc func(a, b K) bool) *BTreeIndex[K, T] {
	return newIndex(keyFunc, lessFunc, false, false)
}

// NewUniqueIndex creates a unique BTree-based index.
func NewUniqueIndex[K comparable, T Entity](keyFunc func(T) K, lessFunc func(a, b K) bool) *BTreeIndex[K, T] {
	return newIndex(keyFunc, lessFunc, true, false)
}

// NewIndexWithLogs creates an index with logging enabled.
func NewIndexWithLogs[K comparable, T Entity](keyFunc func(T) K, lessFunc func(a, b K) bool) *BTreeIndex[K, T] {
	return newIndex(keyFunc, lessFunc, false, true)
}

// NewUniqueIndexWithLogs creates a unique index with logging enabled.
func NewUniqueIndexWithLogs[K comparable, T Entity](keyFunc func(T) K, lessFunc func(a, b K) bool) *BTreeIndex[K, T] {
	return newIndex(keyFunc, lessFunc, true, true)
}

// Helper method for conditional logging
func (idx *BTreeIndex[K, T]) logInfo(ctx context.Context, msg string, args ...any) {
	if idx.logEnabled {
		slog.Default().InfoContext(ctx, msg, args...)
	}
}

// Insert or replace entity in index.
func (idx *BTreeIndex[K, T]) Insert(ctx context.Context, obj T) error {
	idx.logInfo(ctx, "Inserting object into index", "id", obj.GetBase().ID)
	if idx.unique {
		return idx.insertUnique(obj)
	}
	idx.insertNonUnique(obj)
	return nil
}

func (idx *BTreeIndex[K, T]) insertNonUnique(obj T) {
	key := idx.keyFunc(obj)
	it, found := idx.tree.Get(item[K, T]{key: key})

	if found {
		slog.Default().Info("Non-unique key found, adding to collection", "key", key, "id", obj.GetBase().ID)
		collection := it.value
		collection.Store(obj.GetBase().ID, obj)
	} else {
		slog.Default().Info("Key not found, creating new collection", "key", key, "id", obj.GetBase().ID)
		newCollection := &sync.Map{}
		newCollection.Store(obj.GetBase().ID, obj)
		idx.tree.ReplaceOrInsert(item[K, T]{key: key, value: newCollection})
	}
}

func (idx *BTreeIndex[K, T]) insertUnique(obj T) error {
	key := idx.keyFunc(obj)
	slog.Default().Info("Inserting into unique index", "key", key, "id", obj.GetBase().ID)
	it, found := idx.tree.Get(item[K, T]{key: key})

	if found {
		existingMap := it.value
		var existingID uuid.UUID
		existingMap.Range(func(id, _ any) bool {
			existingID = id.(uuid.UUID)
			return false
		})

		if existingID != obj.GetBase().ID {
			slog.Default().Error("Duplicate key detected in unique index", "key", key, "existingID", existingID, "newID", obj.GetBase().ID)
			return ErrDuplicateKey
		}
	}
	newMap := &sync.Map{}
	newMap.Store(obj.GetBase().ID, obj)
	idx.tree.ReplaceOrInsert(item[K, T]{key: key, value: newMap})
	slog.Default().Info("Successfully inserted/updated unique key", "key", key, "id", obj.GetBase().ID)
	return nil
}

// Delete entity from index.
func (idx *BTreeIndex[K, T]) Delete(ctx context.Context, obj T) {
	key := idx.keyFunc(obj)
	idx.logInfo(ctx, "Deleting object from index", "key", key, "id", obj.GetBase().ID)

	existingItem, found := idx.tree.Get(item[K, T]{key: key})
	if found {
		collection := existingItem.value
		collection.Delete(obj.GetBase().ID)
		idx.logInfo(ctx, "Object deleted from collection", "key", key, "id", obj.GetBase().ID)

		isEmpty := true
		collection.Range(func(_, _ any) bool {
			isEmpty = false
			return false
		})

		if isEmpty {
			idx.tree.Delete(existingItem)
			idx.logInfo(ctx, "Collection is empty, removing key from B-Tree", "key", key)
		}
	}
}

// Find returns all entities for a given key.
func (idx *BTreeIndex[K, T]) Find(ctx context.Context, key K) []T {
	idx.logInfo(ctx, "Finding entities by key", "key", key)
	it, found := idx.tree.Get(item[K, T]{key: key})
	if !found {
		idx.logInfo(ctx, "Key not found in index", "key", key)
		return nil
	}
	var results []T
	collection := it.value
	collection.Range(func(_, value any) bool {
		results = append(results, value.(T))
		return true
	})
	idx.logInfo(ctx, "Found entities for key", "key", key, "count", len(results))
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
	idx.logInfo(ctx, "Starting Ascend iteration")
	idx.forRange(ctx, func(btreeFn func(i item[K, T]) bool) {
		idx.tree.Ascend(btreeFn)
	}, fn)
	idx.logInfo(ctx, "Ascend iteration finished")
}

// Descend iterates in descending order.
func (idx *BTreeIndex[K, T]) Descend(ctx context.Context, fn func(T) bool) {
	idx.logInfo(ctx, "Starting Descend iteration")
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.Descend(btreeFn)
		},
		fn)
	idx.logInfo(ctx, "Descend iteration finished")
}

// AscendRange iterates between [lower, upper].
func (idx *BTreeIndex[K, T]) AscendRange(ctx context.Context, lower, upper K, fn func(T) bool) {
	idx.logInfo(ctx, "Starting AscendRange iteration", "lower", lower, "upper", upper)
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.AscendRange(item[K, T]{key: lower}, item[K, T]{key: upper}, btreeFn)
		},
		fn,
	)
	idx.logInfo(ctx, "AscendRange iteration finished")
}

// DescendRange iterates between [upper, lower] in reverse.
func (idx *BTreeIndex[K, T]) DescendRange(ctx context.Context, lower, upper K, fn func(T) bool) {
	idx.logInfo(ctx, "Starting DescendRange iteration", "lower", lower, "upper", upper)
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.DescendRange(item[K, T]{key: upper}, item[K, T]{key: lower}, btreeFn)
		},
		fn,
	)
	idx.logInfo(ctx, "DescendRange iteration finished")
}

// AscendGreaterThanOrEqual iterates from key to the end in ascending order.
func (idx *BTreeIndex[K, T]) AscendGreaterThanOrEqual(ctx context.Context, key K, fn func(T) bool) {
	idx.logInfo(ctx, "Starting AscendGreaterThanOrEqual iteration", "key", key)
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.AscendGreaterOrEqual(item[K, T]{key: key}, btreeFn)
		},
		fn,
	)
	idx.logInfo(ctx, "AscendGreaterThanOrEqual iteration finished")
}

// DescendLessThanOrEqual iterates from key to the beginning in descending order.
func (idx *BTreeIndex[K, T]) DescendLessThanOrEqual(ctx context.Context, key K, fn func(T) bool) {
	idx.logInfo(ctx, "Starting DescendLessThanOrEqual iteration", "key", key)
	idx.forRange(ctx,
		func(btreeFn func(i item[K, T]) bool) {
			idx.tree.DescendLessOrEqual(item[K, T]{key: key}, btreeFn)
		},
		fn,
	)
	idx.logInfo(ctx, "DescendLessThanOrEqual iteration finished")
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
