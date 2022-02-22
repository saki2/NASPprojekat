package WritePath

import (
	"io/ioutil"
	"os"
	"project/structures/SSTable"
	"project/structures/lru"
	"project/structures/memtable"
	wal "project/structures/mmap"
	"strconv"
	"time"
)

var WalSegmentName string  // Path to current Wal segment that gets appended

func WritePath(memtable *memtable.SkipList, cache *lru.Cache, key string, value []byte, SegmentNumElements *uint64) {

	if *SegmentNumElements+1 > wal.SEGMENT_SIZE {	// Wal segment at capacity - new segment is created
		CreateLogFile()
		*SegmentNumElements = 0
	}
	err := wal.Add(key, value, WalSegmentName, false)
	if err == nil { 		// Commit log confirmed entry
		*SegmentNumElements += 1
		_, found := cache.Find(key)
		if found {
			cache.Update(key, value, uint64(time.Now().Unix()), false)
		}
		forFlush := memtable.Insert(key, value, time.Now().Unix())
		if forFlush != nil {			// Memtable up to capacity, flush to disk
			SSTable.Flush(forFlush)
			memtable.NewSkipList()		// Reset memtable
			wal.DeleteWal()
		}
	}

}

// CreateLogFile : Wal directory has n segments. Function creates (n+1)th segment to be current segment for appending
func CreateLogFile() {
	files, err := ioutil.ReadDir("./Wal")
	if err != nil {
		panic(err.Error())
	}
	offset := strconv.Itoa(len(files)+1)
	file, err := os.Create("./Wal"+ "/" + "wal_" + offset + ".db")
	defer file.Close()
	if err != nil {
		panic(err.Error())
	}
	WalSegmentName = "./Wal"+ "/" +"wal_" + offset + ".db"
}