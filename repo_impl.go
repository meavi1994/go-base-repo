package base_repo

import (
	"errors"
	"fmt"
	"github.com/google/btree"
	"github.com/google/uuid"
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
	GetById(id uuid.UUID) (T, error)
	GetAllByIds(ids []uuid.UUID) (map[uuid.UUID]T, error)
	Update(id uuid.UUID, val T) error
	Delete(id uuid.UUID) error
	Exists(id uuid.UUID) bool
	Count() int
	ListAll() []T
	Find(predicate func(T) bool) []T
	Upsert(val T) (uuid.UUID, error)
	CompareAndSwap(id uuid.UUID, oldVal T, newVal T) (bool, error)
	GetIndex(name string) (*btree.BTreeG[T], bool)
	AddIndex(name string, lessFn func(a, b T) bool) error
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
	indexes     map[string]*btree.BTreeG[T]
	indexLocks  map[string]*sync.Mutex
	subscribers []chan *Event[T]
}

// Constructor
func NewBaseRepo[T Entity](indexes map[string]*btree.BTreeG[T], subscribers []chan *Event[T]) BaseRepo[T] {
	locks := make(map[string]*sync.Mutex, len(indexes))
	for name := range indexes {
		locks[name] = &sync.Mutex{}
	}
	return &baseRepo[T]{
		indexes:     indexes,
		indexLocks:  locks,
		subscribers: subscribers,
	}
}

// ----------------- Internal Helpers -----------------

func (a *baseRepo[T]) addToIndexes(val T) {
	for name, index := range a.indexes {
		lock := a.indexLocks[name]
		lock.Lock()
		index.ReplaceOrInsert(val)
		lock.Unlock()
	}
}

func (a *baseRepo[T]) removeFromIndexes(val T) {
	for name, index := range a.indexes {
		lock := a.indexLocks[name]
		lock.Lock()
		index.Delete(val)
		lock.Unlock()
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

func (a *baseRepo[T]) GetById(id uuid.UUID) (T, error) {
	var zero T
	if val, ok := a.values.Load(id); ok {
		return val.(T), nil
	}
	return zero, errors.New("entity not found")
}

func (a *baseRepo[T]) GetAllByIds(ids []uuid.UUID) (map[uuid.UUID]T, error) {
	result := make(map[uuid.UUID]T, len(ids))
	for _, id := range ids {
		if val, ok := a.values.Load(id); ok {
			result[id] = val.(T)
		}
	}
	return result, nil
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

func (a *baseRepo[T]) Exists(id uuid.UUID) bool {
	_, ok := a.values.Load(id)
	return ok
}

func (a *baseRepo[T]) Count() int {
	count := 0
	a.values.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (a *baseRepo[T]) ListAll() []T {
	result := []T{}
	a.values.Range(func(_, v any) bool {
		result = append(result, v.(T))
		return true
	})
	return result
}

func (a *baseRepo[T]) Find(predicate func(T) bool) []T {
	result := []T{}
	a.values.Range(func(_, v any) bool {
		entity := v.(T)
		if predicate(entity) {
			result = append(result, entity)
		}
		return true
	})
	return result
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

// ----------------- Index Access -----------------

func (a *baseRepo[T]) GetIndex(name string) (*btree.BTreeG[T], bool) {
	index, ok := a.indexes[name]
	return index, ok
}

// AddIndex creates and registers a new index with the given name and comparator
func (a *baseRepo[T]) AddIndex(name string, lessFn func(a, b T) bool) error {
	if _, exists := a.indexes[name]; exists {
		return fmt.Errorf("index with name %q already exists", name)
	}

	tree := btree.NewG(2, lessFn)
	a.indexes[name] = tree
	a.indexLocks[name] = &sync.Mutex{}

	// Populate index with existing values
	a.values.Range(func(_, v any) bool {
		tree.ReplaceOrInsert(v.(T))
		return true
	})

	return nil
}

// ----------------- String -----------------

func (a *baseRepo[T]) String() string {
	var all []string
	for _, el := range a.ListAll() {
		all = append(all, fmt.Sprintf("%v", el))
	}
	return fmt.Sprintf("[%v]", strings.Join(all, ","))
}
