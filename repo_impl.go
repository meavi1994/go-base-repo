package base_repo

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	functional "github.com/meavi1994/go-functional"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// ----------------- BaseModel -----------------

type BaseModel struct {
	ID        uuid.UUID
	CreatedAt time.Time
	UpdatedAt time.Time
}

// GetBase satisfies Entity interface
func (b BaseModel) GetBase() *BaseModel {
	return &b
}

// ----------------- Entity & Repo Interfaces -----------------

type Entity interface {
	GetBase() *BaseModel
}

type BaseRepo[T Entity] interface {
	Add(val T) (uuid.UUID, error)
	Upsert(val T) (uuid.UUID, error)
	Update(id uuid.UUID, val T) error
	Delete(id uuid.UUID) error
	CompareAndSwap(id uuid.UUID, oldVal T, newVal T) (bool, error)

	GetById(id uuid.UUID) (T, error)
	GetAllByIds(ids []uuid.UUID) (map[uuid.UUID]T, error)
	Exists(id uuid.UUID) bool
	Count() int
	ListAll() []T
	Find(predicate func(T) bool) []T

	GetIndex(name string) (BTreeG[T], bool)
	String() string
}

// ----------------- Event -----------------

type Event[T Entity] struct {
	ID   uuid.UUID
	Type string
	Val  T
}

// ----------------- Base Repo -----------------

type baseRepo[T Entity] struct {
	values      sync.Map
	indexes     map[string]BTreeG[T]
	subscribers []chan *Event[T]
}

// Constructor
func NewBaseRepo[T Entity](indexes map[string]BTreeG[T], subscribers []chan *Event[T]) BaseRepo[T] {
	locks := make(map[string]*sync.Mutex, len(indexes))
	for name := range indexes {
		locks[name] = &sync.Mutex{}
	}
	return &baseRepo[T]{
		indexes:     indexes,
		subscribers: subscribers,
	}
}

// ----------------- Internal Helpers -----------------

func (a *baseRepo[T]) addToIndexes(val T) {
	for _, index := range a.indexes {
		index.ReplaceOrInsert(val)
	}
}

func (a *baseRepo[T]) removeFromIndexes(val T) {
	for _, index := range a.indexes {
		index.Delete(val)
	}
}

// notify creates Event from type and val
func (a *baseRepo[T]) notify(eventType string, val T) {
	e := &Event[T]{
		ID:   uuid.New(),
		Type: eventType,
		Val:  val,
	}
	for _, subscriber := range a.subscribers {
		subscriber <- e
	}
}

// ----------------- Public Methods -----------------

func (a *baseRepo[T]) Add(val T) (uuid.UUID, error) {
	now := time.Now()
	baseModel := val.GetBase()
	baseModel.ID = uuid.New()
	baseModel.CreatedAt = now
	baseModel.UpdatedAt = now

	a.values.Store(baseModel.ID, val)
	a.addToIndexes(val)
	a.notify("create", val)

	return baseModel.ID, nil
}

func (a *baseRepo[T]) Upsert(val T) (uuid.UUID, error) {
	baseModel := val.GetBase()
	if baseModel.ID == uuid.Nil {
		return a.Add(val)
	}

	baseModel.UpdatedAt = time.Now()
	a.values.Store(baseModel.ID, val)
	a.addToIndexes(val)
	a.notify("upsert", val)

	return baseModel.ID, nil
}

func (a *baseRepo[T]) Update(id uuid.UUID, val T) error {
	_, ok := a.values.Load(id)
	if !ok {
		return errors.New("entity not found")
	}

	val.GetBase().UpdatedAt = time.Now()
	a.values.Store(id, val)
	a.addToIndexes(val)
	a.notify("update", val)

	return nil
}

func (a *baseRepo[T]) Delete(id uuid.UUID) error {
	val, ok := a.values.Load(id)
	if !ok {
		return errors.New("entity not found")
	}

	a.values.Delete(id)
	a.removeFromIndexes(val.(T))
	a.notify("delete", val.(T))

	return nil
}

// ----------------- CompareAndSwap -----------------

func (a *baseRepo[T]) CompareAndSwap(id uuid.UUID, oldVal T, newVal T) (swapped bool, err error) {
	swapped = a.values.CompareAndSwap(id, oldVal, newVal)
	if !swapped {
		_, exists := a.values.Load(id)
		if !exists {
			return false, errors.New("entity not found")
		}
		return false, nil
	}

	newVal.GetBase().UpdatedAt = time.Now()
	a.removeFromIndexes(oldVal)
	a.addToIndexes(newVal)
	a.notify("update", newVal)

	return true, nil
}

// -- read methods

func (a *baseRepo[T]) GetById(id uuid.UUID) (T, error) {
	var zero T
	if val, ok := a.values.Load(id); ok {
		return val.(T), nil
	}
	return zero, errors.New("entity not found")
}

func (a *baseRepo[T]) GetAllByIds(ids []uuid.UUID) (map[uuid.UUID]T, error) {
	return maps.Collect(
		functional.SyncGetAllByKeys[uuid.UUID, T](&a.values, ids),
	), nil
}

func (a *baseRepo[T]) Exists(id uuid.UUID) bool {
	_, ok := a.values.Load(id)
	return ok
}

func (a *baseRepo[T]) Count() int {
	count := 0
	for _, _ = range a.values.Range {
		count++
	}
	return count
}

func (a *baseRepo[T]) ListAll() []T {
	return slices.Collect(functional.SyncValues[uuid.UUID, T](&a.values))
}

func (a *baseRepo[T]) Find(predicate func(T) bool) []T {
	return slices.Collect[T](
		functional.Filter(
			functional.SyncValues[uuid.UUID, T](&a.values),
			predicate),
	)
}

// ----------------- Index Access -----------------

func (a *baseRepo[T]) GetIndex(name string) (BTreeG[T], bool) {
	index, ok := a.indexes[name]
	return index, ok
}

// ----------------- String -----------------

func (a *baseRepo[T]) String() string {
	var all []string
	for _, el := range a.ListAll() {
		all = append(all, fmt.Sprintf("%v", el))
	}
	return fmt.Sprintf("[%v]", strings.Join(all, ","))
}
