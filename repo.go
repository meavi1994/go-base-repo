// Package base_repo file: repo.go

package base_repo

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"sync"
	"time"
)

var ErrNotFound = errors.New("not found")
var ErrDuplicateKey = errors.New("duplicate key on unique index")

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

type Event[T any] struct {
	Type string // "create", "update", "delete", "cas"
	Obj  T
}

// Repo interface (public contract).
type Repo[T Entity] interface {
	Create(ctx context.Context, obj T) (uuid.UUID, error)
	Get(ctx context.Context, id uuid.UUID) (T, error)
	GetAllByIds(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]T, error)
	Update(ctx context.Context, obj T) error
	Delete(ctx context.Context, id uuid.UUID) error
	GetAll(ctx context.Context) []T
	WithStore(fn func(store *sync.Map))

	// Index support
	AddIndex(name string, idx Indexer[T])
	GetIndex(name string) (Indexer[T], bool)

	// Subscribers
	AddSubscriber(chan Event[T])
}
