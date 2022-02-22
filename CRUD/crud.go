package CRUD

import (
	"project/structures/LSM"
	"project/structures/ReadPath"
	"project/structures/WritePath"
	"project/structures/lru"
	"project/structures/memtable"
	"time"
)

/* CRUD takes in which function will be performed
"c" = create
"r" = read
"u" = update
"d" = delete
*/

func Create(mem *memtable.SkipList, key string, value []byte, SegmentNumElements *uint64) {
	WritePath.WritePath(mem, key, value, SegmentNumElements)
}

func Read(mem *memtable.SkipList, cache *lru.Cache, key string) *ReadPath.ElementInfo {
	element := ReadPath.ReadPath(mem, cache, key)
	return element
}

func Update(mem *memtable.SkipList, key string, value []byte, SegmentNumElements *uint64) {
	WritePath.WritePath(mem, key, value, SegmentNumElements)
}

func Delete(mem *memtable.SkipList, key string) *memtable.SkipList {
	// If key exists in memtable, tombstone is put to true
	deleted := mem.Delete(key)
	// If key doesn't exist in memtable it is first added than deleted
	if !deleted {
		mem.Insert(key, []byte(""), time.Now().Unix())
		mem.Delete(key)
	}
	return mem
}

func Compact() {
	LSM.Compactions()
}
