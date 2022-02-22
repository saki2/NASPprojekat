package CRUD

import (
	"project/structures/LSM"
	"project/structures/ReadPath"
	"project/structures/SSTable"
	"project/structures/WritePath"
	"project/structures/lru"
	"project/structures/memtable"
	wal "project/structures/mmap"
	"time"
)

/* CRUD takes in which function will be performed
"c" = create
"r" = read
"u" = update
"d" = delete
*/

func Create(mem *memtable.SkipList, cache *lru.Cache, key string, value []byte) {
	WritePath.WritePath(mem, cache, key, value)
}

func Read(mem *memtable.SkipList, cache *lru.Cache, key string) *ReadPath.ElementInfo {
	element := ReadPath.ReadPath(mem, cache, key)
	return element
}

func Update(mem *memtable.SkipList, cache *lru.Cache, key string, value []byte) {
	WritePath.WritePath(mem, cache, key, value)
}

func Delete(mem *memtable.SkipList, cache *lru.Cache, key string) bool {

	if WritePath.SegmentElements > wal.SEGMENT_SIZE {	// Wal segment at capacity - new segment is created
		WritePath.CreateLogFile()
		WritePath.SegmentElements = 0
	}
	err := wal.Add(key, []byte(""), WritePath.WalSegmentName, true)
	if err == nil { 		// Commit log confirmed entry
		WritePath.SegmentElements += 1
		_, found := cache.Find(key)
		if found {
			cache.Update(key, []byte(""), uint64(time.Now().Unix()), true)
		}
		// If key exists in memtable, tombstone is put to true
		deleted := mem.Delete(key)
		// If key doesn't exist in memtable it is first added than deleted
		if !deleted {
			a := mem.Insert(key, []byte(""), time.Now().Unix())
			mem.Delete(key)
			if a != nil {			// Memtable up to capacity, flush to disk
				SSTable.Flush(a)
				mem.NewSkipList()		// Reset memtable
				wal.DeleteWal()
			}
		}
		return true
	}
	return false
}

func Compact() {
	LSM.Compactions()
}
