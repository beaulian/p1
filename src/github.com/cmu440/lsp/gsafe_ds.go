package lsp

import (
    "sync"
    "errors"
    "container/list"
)

// 互斥锁一种实现方式,使用Channel实现
//============================
// type Mutex struct {
// 	sync chan int
// }
//
// func NewMutex() *Mutex {
// 	sync := make(chan int, 1)
// 	sync <- 1
// 	return &Mutex{ sync }
// }
//
// func (m *Mutex) Lock() {
// 	<- m.sync
// }
//
// func (m *Mutex ) Unlock() {
// 	m.sync <- 1
// }
//============================

type Queue struct {
    m *sync.Mutex
    list *list.List
}

func NewQueue() *Queue {
    return &Queue{ new(sync.Mutex), &list.List{} }
}

func (q *Queue) Push(value interface{}) {
    q.m.Lock()
    defer q.m.Unlock()

    q.list.PushBack(value)
}

func (q *Queue) Pop() interface{} {
    q.m.Lock()
    defer q.m.Unlock()

    head := q.list.Front()
    if head != nil {
        return head.Value
    }
    return nil
}

func (q *Queue) Len() int {
    q.m.Lock()
    defer q.m.Unlock()

    return q.list.Len()
}

func (q *Queue) Values(offset int) ([]interface{}, error) {
    q.m.Lock()
    defer q.m.Unlock()

    len := q.list.Len()
    if offset >= len {
        return nil, errors.New("list out of index")
    }
    values := make([]interface{}, len - offset)
    e := q.list.Front()
    for offset > 0 {
        e = e.Next()
        offset--
    }
    for e != nil {
        values = append(values, e.Value)
        e = e.Next()
    }
    return values, nil
}

type SyncMap struct {
    m *sync.Mutex
    mapper map[int]interface{}
}

func NewSyncMap() *SyncMap {
    return &SyncMap{ new(sync.Mutex), make(map[int]interface{}, 1) }
}

func (a *SyncMap) Set(key int, value interface{}) {
    a.m.Lock()
    defer a.m.Unlock()

    a.mapper[key] = value
}

func (a *SyncMap) Get(key int) interface{} {
    a.m.Lock()
    defer a.m.Unlock()

    return a.mapper[key]
}

type SyncCounter struct {
    m *sync.Mutex
    counter int
}

func NewSyncCounter(count int) *SyncCounter {
    return &SyncCounter{ new(sync.Mutex), count }
}

func (a *SyncCounter) Inc() {
    a.m.Lock()
    defer a.m.Unlock()

    a.counter++
}

func (a *SyncCounter) Dec() {
    a.m.Lock()
    defer a.m.Unlock()

    a.counter--
}

func (a *SyncCounter) Value() int {
    a.m.Lock()
    defer a.m.Unlock()

    return a.counter
}

func (a *SyncCounter) Set(num int) {
    a.m.Lock()
    defer a.m.Unlock()

    a.counter = num
}

type SyncBool struct {
	m *sync.Mutex
	bValue	bool
}

func NewSyncBool(bValue bool) *SyncBool {
	return &SyncBool{ new(sync.Mutex), bValue }
}

func (a *SyncBool) Value() bool {
	a.m.Lock()
	defer a.m.Unlock()
	return a.bValue
}

func (a *SyncBool) Set(value bool) {
	a.m.Lock()
	defer a.m.Unlock()
	a.bValue = value
}
