package lru

import (
	"container/list"
	"fmt"
)

const (
	DEFAULT_CAPACITY = 10
)

type Pair struct {
	Key 	string
	Value 	[]byte
}

type Cache struct {
	capacity		int
	data			*list.List					// Lista cuva par kljuc-vrednost, najskoriji element je na kraju
	dataMap			map[string]*list.Element	// Kljuc i pokazivac na element u listi
}

// NewCache : Konstruktor LRU cache sa default kapacitetom
func NewCache() *Cache {
	c := new(Cache)
	c.capacity = DEFAULT_CAPACITY
	c.dataMap = make(map[string]*list.Element)
	c.data = list.New()
	return c
}

// SetCapacity : Podesavanje kapaciteta
func (cache *Cache) SetCapacity(c int) {
	cache.capacity = c
}

// Find : Trazenje podatka u kesu - vraca vrednost ili nil i bool da li je uspesno nadjen
func (cache *Cache) Find(key string) ([]byte, bool) {
	listItem, found := cache.dataMap[key]
	if found {
		p := listItem.Value.(Pair)
		return p.Value, true
	} else {
		return nil, false
	}
}

// Add : Dodavanje elementa u kes
func (cache *Cache) Add(key string, value []byte) {
	p := Pair{key, value}
	listItem, found := cache.dataMap[p.Key]
	if found {
		cache.data.MoveToBack(listItem)
	} else {	// Element se ne nalazi u kesu
		if cache.data.Len()+1 <= cache.capacity {	// Kes nije popunjen
			cache.data.PushBack(p)
		} else {
			// Kes je popunjen i uklanja se prvi element iz liste (i recnika) - onaj kome se davno pristupalo
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


