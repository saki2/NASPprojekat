package WritePath

import (
	"io/ioutil"
	"os"
	"project/structures/SSTable"
	"project/structures/memtable"
	wal "project/structures/mmap"
	"strconv"
	"time"
)

var WalSegmentName string  // Path to current Wal segment that gets appended

func WritePath(memtable *memtable.SkipList, key string, value []byte, SegmentNumElements *uint64) {

	// Wal segment at capacity - new segment is created
	if *SegmentNumElements+1 > wal.SEGMENT_SIZE {
		CreateLogFile()
		*SegmentNumElements = 0
	}
	err := wal.Add(key, value, WalSegmentName)
	*SegmentNumElements += 1
	if err == nil {
		// Commit log confirmed entry
		forFlush := memtable.Insert(key, value, time.Now().Unix())
		if forFlush != nil {	// Memtable up to capacity, flush to disk
			SSTable.Flush(forFlush)
			memtable.NewSkipList()		// Reset memtable
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
	_, err = os.Create("./Wal"+ "/" + "wal_" + offset + ".db")
	if err != nil {
		panic(err.Error())
	}
	WalSegmentName = "./Wal"+ "/" +"wal_" + offset + ".db"
}