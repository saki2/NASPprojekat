package main

import (
	"fmt"
	"project/structures/Initialization"
	"project/structures/TokenBucket"
	"project/structures/WritePath"
	"project/structures/memtable"
	wal "project/structures/mmap"
	"time"
)


func main() {

	Initialization.Configure()
	Initialization.CreateDataFiles()

	memtableInstance := memtable.SkipList{}
	memtableInstance.NewSkipList()
	//cache := lru.NewCache()
	TokenBucketInstance := TokenBucket.NewTokenBucket()
	WritePath.WalSegmentName = wal.ScanWal(&memtableInstance)
	if memtableInstance.Size == 0 {		// There is no leftover data from logs
		WritePath.CreateLogFile()
		WritePath.SegmentElements = 0
	} else {
		WritePath.SegmentElements = uint64(wal.CalculateSegmentSize(WritePath.WalSegmentName))
		fmt.Println(WritePath.SegmentElements)
	}

	lastReset := Now()
	availableReq := TokenBucketInstance.MaxReq
	if Now()-lastReset >= TokenBucketInstance.Interval {	// Interval has passed, counters are reset
		lastReset = Now()
		availableReq = TokenBucketInstance.MaxReq
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
