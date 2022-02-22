package lru

import (
	"container/list"
	"fmt"
)

var CAPACITY int

const (
	DEFAULT_CAPACITY = 10
)

type Pair struct {
	Key   string
	Value Information
}

type Cache struct {
	capacity int
	data     *list.List // Contains Pair objects, sorted by least recently used to most recently
	dataMap  map[string]*list.Element
}

type Information struct {
	Key       string
	Value     []byte
	Timestamp uint64
	Tombstone bool
}

func NewCache() *Cache {
	c := new(Cache)
	c.capacity = CAPACITY
	c.dataMap = make(map[string]*list.Element)
	c.data = list.New()
	return c
}

func (cache *Cache) ContainsKey(key string) {
}

func SetDefaultParam() {
	CAPACITY = DEFAULT_CAPACITY
}

func (cache *Cache) PrintCapacity() {
	fmt.Println(cache.capacity)
}

// Find : Returns Information object or nil and bool depending on whether the element was found by key
func (cache *Cache) Find(key string) (*Information, bool) {
	listItem, found := cache.dataMap[key]
	if found {
		p := listItem.Value.(Pair)
		if p.Value.Tombstone != true {
			return &p.Value, true
		} else {
			return nil, false
		}
	} else {
		return nil, false
	}
}

func (cache *Cache) Add(key string, info Information) {
	p := Pair{key, info}
	listItem, found := cache.dataMap[p.Key]
	if found {
		cache.data.MoveToBack(listItem)
	} else { // Element not in cache
		if cache.data.Len()+1 <= cache.capacity { // Cache not up to capacity
			cache.data.PushBack(p)
		} else {
			// Cache at full capacity - least recent element is removed from cache
			rem := cache.data.Front()
			delete(cache.dataMap, rem.Value.(Pair).Key)
			cache.data.Remove(rem)
			cache.data.PushBack(p)
		}
		cache.dataMap[p.Key] = cache.data.Back()
	}

}

func (cache *Cache) Check() {
	for e := cache.data.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
}
