package main

import (
	"fmt"
	"project/structures/Bloom_Filter"
	"project/structures/Configuration"
	"project/structures/WritePath"
	"project/structures/lru"
	"project/structures/memtable"
	wal "project/structures/mmap"
	"time"
)

const (
	DEFAULT_MAX_REQUEST = 20
	DEFAULT_INTERVAL = 10
)


func main() {

	var maxreq int
	var interval int64
	//segmentNumElem := uint64(0)

	config := Configuration.LoadConfig()
	// Extract configuration values
	if config != nil {
		wal.SEGMENT_SIZE = config.WalSegmentSize
		memtable.CAPACITY = config.MemtableCapacity
		memtable.MAX_HEIGHT = config.MemtableMaxHeight
		bloom_filter.FALSE_POSITIVE_RATE = config.BloomFalsePositiveRate
		lru.CAPACITY = config.LRUCapacity
		maxreq = config.MaxRequestPerInterval
		interval = config.Interval
	} else {	// Configurational file is non-existent, resort to default values
		wal.SetDefaultParam()
		memtable.SetDefaultParam()
		bloom_filter.SetDefaultParam()
		lru.SetDefaultParam()
		maxreq = DEFAULT_MAX_REQUEST
		interval = DEFAULT_INTERVAL
	}

	WritePath.CreateLogFile()
	memtableInstance := memtable.SkipList{}
	memtableInstance.NewSkipList()
	//cache := lru.NewCache()

	lastReset := Now()
	availableReq := maxreq
	if Now()-lastReset >= interval {	// Interval has passed, counters are reset
		lastReset = Now()
		availableReq = maxreq
		fmt.Println("Interval reset")
		// salje se zahtev
		availableReq -= 1
	} else {
		if availableReq-1 > 0 {
			fmt.Println("In interval")
			// salje se zahtev
			availableReq -= 1
		} else {
			fmt.Println("Too many requests for the set time interval")
		}
	}
}

func Now() int64 {
	return time.Now().Unix()
}
