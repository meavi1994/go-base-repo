package base_repo

import "github.com/google/btree"

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
