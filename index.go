// Package base_repo file: index.go
package base_repo

import (
	"sync"
)

// item wraps a string key and a collection of entities.
// A sync.Map is used to handle multiple entities with the same key.
type item[T Entity] struct {
	key string
	// The value is a map from the entity's unique ID to the entity itself.
	value *sync.Map
}

// Index contract for ordered indexes over entities.
type Index[T Entity] interface {
	Insert(obj T) error
	Delete(obj T)
	Find(key string) []T
	Ascend(fn func(T) bool)
	Descend(fn func(T) bool)
	AscendRange(lower, upper string, fn func(T) bool)
	DescendRange(lower, upper string, fn func(T) bool)
	AscendGreaterThanOrEqual(key string, fn func(T) bool)
	DescendLessThanOrEqual(key string, fn func(T) bool)
}
