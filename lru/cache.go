package lru

import (
	"bufio"
	"container/list"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	CAPACITY = 50
)

type Pair struct {
	Key 	string
	Value 	[]byte
}

type Cache struct {
	capacity		int
	data			*list.List		// Contains Pair objects, sorted by least recently used to most recently
	dataMap			map[string]*list.Element
}

func NewCache() *Cache  {
	c := new(Cache)
	c.loadConfig()
	c.dataMap = make(map[string]*list.Element)
	c.data = list.New()
	return c
}

func (cache *Cache) loadConfig() {
	file, err := os.OpenFile("lru/config.txt", os.O_RDONLY, 0700)
	if errors.Is(err, os.ErrNotExist) {
		cache.capacity = CAPACITY
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()
	line := scanner.Text()
	split := strings.Split(line, "=")
	c, err := strconv.Atoi(split[1])
	if err == nil {
		cache.capacity = c
	} else {
		// Config file format incorrect, cache capacity set to default
		cache.capacity = CAPACITY
	}
}

// Find : Returns value or nil and bool depending on whether the element was found by key
func (cache *Cache) Find(key string) ([]byte, bool) {
	listItem, found := cache.dataMap[key]
	if found {
		p := listItem.Value.(Pair)
		return p.Value, true
	} else {
		return nil, false
	}
}

func (cache *Cache) Add(key string, value []byte) {
	p := Pair{key, value}
	listItem, found := cache.dataMap[p.Key]
	if found {
		listItem.Value = p
		cache.data.MoveToBack(listItem)
	} else {	// Element not in cache
		if cache.data.Len()+1 <= cache.capacity {	// Cache not up to capacity
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
	fmt.Println()
}


