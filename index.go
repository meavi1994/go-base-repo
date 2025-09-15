// Package base_repo file: index.go
package base_repo

import (
	"sync"
)

// item wraps a string key and a collection of entities.
// A sync.Map is used to handle multiple entities with the same key.
type item[K comparable, T Entity] struct {
	key K
	// The value is a map from the entity's unique ID to the entity itself.
	value *sync.Map
}

type Indexer[T Entity] interface {
	Insert(obj T) error
	Delete(obj T)
	Ascend(fn func(T) bool)
	Descend(fn func(T) bool)
}

// Index contract for ordered indexes over entities.
type Index[K comparable, T Entity] interface {
	Insert(obj T) error
	Delete(obj T)
	Find(key K) []T
	Ascend(fn func(T) bool)
	Descend(fn func(T) bool)
	AscendRange(lower, upper K, fn func(T) bool)
	DescendRange(lower, upper K, fn func(T) bool)
	AscendGreaterThanOrEqual(key K, fn func(T) bool)
	DescendLessThanOrEqual(key K, fn func(T) bool)
}
