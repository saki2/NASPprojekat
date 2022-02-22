package Initialization

import (
	"os"
	bloom_filter "project/structures/Bloom_Filter"
	"project/structures/Configuration"
	"project/structures/LSM"
	"project/structures/TokenBucket"
	"project/structures/lru"
	"project/structures/memtable"
	wal "project/structures/mmap"
	"strconv"
)

func CreateDataFiles() {
	// Creates the original directories where data will be stored
	// Function is only called once when the system starts
	if _, err := os.Stat("./Data"); os.IsNotExist(err) {
		err := os.Mkdir("./Data", 0755)
		if err != nil {
			panic(err.Error())
		}
		if _, err := os.Stat("./Data/SSTable"); os.IsNotExist(err) {
			err := os.Mkdir("./Data/SSTable", 0755)
			if err != nil {
				panic(err.Error())
			}
			for i := 1; i <= LSM.MAX_LEVEL; i++ {
				if _, err := os.Stat("./Data/SSTable/Level" + strconv.Itoa(i)); os.IsNotExist(err) {
					err := os.Mkdir("./Data/SSTable/Level"+strconv.Itoa(i), 0755)
					if err != nil {
						panic(err.Error())
					}
				}
			}
		}
	}
	if _, err := os.Stat("./Wal") ; os.IsNotExist(err) {
		err := os.Mkdir("./Wal", 0755)
		if err != nil {
			panic(err.Error())
		}
	}
}

func Configure() {
	config := Configuration.LoadConfig()
	// Extract configuration values
	if config != nil {
		wal.SEGMENT_SIZE = config.WalSegmentSize
		memtable.CAPACITY = config.MemtableCapacity
		memtable.MAX_HEIGHT = config.MemtableMaxHeight
		bloom_filter.FALSE_POSITIVE_RATE = config.BloomFalsePositiveRate
		lru.CAPACITY = config.LRUCapacity
		LSM.MAX_LEVEL = config.LSMMaxLevel
		TokenBucket.MAX_REQ = config.MaxRequestPerInterval
		TokenBucket.INTERVAL = config.Interval
	} else {	// Configuration file is non-existent, resort to default values
		wal.SetDefaultParam()
		memtable.SetDefaultParam()
		bloom_filter.SetDefaultParam()
		lru.SetDefaultParam()
		LSM.SetDefaultParam()
		TokenBucket.SetDefaultParam()
	}
}
