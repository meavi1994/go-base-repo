// Package base_repo file: index.go
package base_repo

import (
	"context"
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
	Insert(ctx context.Context, obj T) error
	Delete(ctx context.Context, obj T)
	Ascend(ctx context.Context, fn func(T) bool)
	Descend(ctx context.Context, fn func(T) bool)
}

// Index contract for ordered indexes over entities.
type Index[K comparable, T Entity] interface {
	Insert(ctx context.Context, obj T) error
	Delete(ctx context.Context, obj T)
	Find(ctx context.Context, key K) []T
	Ascend(ctx context.Context, fn func(T) bool)
	Descend(ctx context.Context, fn func(T) bool)
	AscendRange(ctx context.Context, lower, upper K, fn func(T) bool)
	DescendRange(ctx context.Context, lower, upper K, fn func(T) bool)
	AscendGreaterThanOrEqual(ctx context.Context, key K, fn func(T) bool)
	DescendLessThanOrEqual(ctx context.Context, key K, fn func(T) bool)
}
