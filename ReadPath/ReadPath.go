package ReadPath

import (
	"encoding/binary"
	"hash/crc32"
	"io/ioutil"
	"project/structures/Bloom_Filter"
	"project/structures/SSTable"
	"project/structures/lru"
	"project/structures/memtable"
)

func Panic(err error) {
	panic(err.Error())
}

// If key is found ElementInfo will be returned from ReadPath call

type ElementInfo struct {
	CRC       uint32
	Timestamp uint64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func CheckMemtable(sl *memtable.SkipList, key string) (*ElementInfo, *lru.Information) {

	node := sl.FindNode(key)
	if node != nil {
		// If key found in memtable Element info is created to be returned to the user
		EI := ElementInfo{}
		EI.Timestamp = binary.LittleEndian.Uint64(node.TimeStamp)
		EI.Tombstone = node.Tombstone
		EI.KeySize = uint64(len([]byte(key)))
		EI.Key = key
		EI.ValueSize = uint64(len(node.Value))
		EI.Value = node.Value

		// Cache info is being created so it can be written inside the cache
		cacheInfo := lru.Information{}
		cacheInfo.Key = key
		cacheInfo.Value = node.Value
		cacheInfo.Tombstone = node.Tombstone
		cacheInfo.Timestamp = binary.LittleEndian.Uint64(node.TimeStamp)

		return &EI, &cacheInfo
	}
	return nil, nil
}

func CheckCache(c *lru.Cache, key string)  *ElementInfo{
	element, found := c.Find(key)
	if found{
		// If key is found inside Cache it is brought to the beginning of the Cache
		c.Add(key, *element)
		// ElementInfo object is created based on information from the cache
		EI := ElementInfo{}
		EI.Timestamp = uint64(int64(element.Timestamp))
		EI.Tombstone = element.Tombstone
		EI.KeySize = uint64(len([]byte(key)))
		EI.Key = key
		EI.ValueSize = uint64(len(element.Value))
		EI.Value = element.Value
		return &EI
	}
	return  nil
}

func CheckBloomFilter(path, key string) bool {
	bf := bloom_filter.ReadBloomFilter(path)
	return bf.Contains(key)
}

func CheckSummary(path, key string) (bool, int64) {
	offset := SSTable.ReadSummary(path, key)
	if offset != -1 {
		return true, offset
	} else {
		return false, offset
	}
}

func CheckIndex(path, key string, offset int64) (bool, int64) {
	offset2 := SSTable.ReadIndex(path, key, offset)
	if offset2 != -1 {
		return true, offset
	} else {
		return false, offset
	}
}

func CheckData(path, key string, offset int64) (*ElementInfo, *lru.Information) {
	crc, timeStamp, tombStone, keySize, valueSize, currentKey, value := SSTable.ReadData(path, key, offset)
	if binary.LittleEndian.Uint32(crc) != crc32.ChecksumIEEE(value) {
		// If Checksum doesn't add up error is raised
		panic("Data corrupted")
	} else {
		EI := ElementInfo{}
		EI.CRC = binary.LittleEndian.Uint32(crc)
		EI.Timestamp = binary.LittleEndian.Uint64(timeStamp)
		var ts bool
		if tombStone[0] == 1 {
			ts = true
		} else {
			ts = false
		}
		EI.Tombstone = ts
		EI.KeySize = binary.LittleEndian.Uint64(keySize)
		EI.ValueSize = binary.LittleEndian.Uint64(valueSize)
		EI.Key = string(currentKey)
		EI.Value = value

		// Cache info is being created, so it can be written inside the cache
		cacheInfo := lru.Information{}
		cacheInfo.Key = key
		cacheInfo.Value = value
		cacheInfo.Tombstone = ts
		cacheInfo.Timestamp = binary.LittleEndian.Uint64(timeStamp)
		return &EI, &cacheInfo
	}
}

func ReadPath(memtable *memtable.SkipList, cache *lru.Cache, key string) *ElementInfo {

	// First we check the MemTable
	foundMemtable, cacheInfo := CheckMemtable(memtable, key)
	if foundMemtable != nil {
		// If key is found in Memtable, it is written at the front of the Cache
		cache.Add(key, *cacheInfo)
		return foundMemtable
	}
	// If it's not in the Memtable we check the Cache
	foundCache := CheckCache(cache, key)
	if foundCache != nil {
		return foundCache
	}
	// If key is not found in the memory we check the SSTables on the disk
	// A list of all SSTables is created
	files, err := ioutil.ReadDir("./Data/SSTable")
	Panic(err)
	// We go through the list of SSTables
	for _, file := range files {
		fileNum := file.Name()[7:] // Get the number of the SSTable
		// First we load the BloomFilter and check if it MIGHT contain the key
		if CheckBloomFilter("Data/SSTable/SSTable"+fileNum+"/usertable-"+fileNum+"-Filter.db", key) {
			// If it does, we then check the Summary of the SSTable
			foundSummary, offsetIndex := CheckSummary("Data/SSTable/SSTable"+fileNum+"/usertable-"+fileNum+"-Summary.db", key)
			// If the key is inside the Summary we find the offset in the index file and the data file
			if foundSummary {
				foundIndex, offsetData := CheckIndex("Data/SSTable/SSTable"+fileNum+"/usertable-"+fileNum+"-Index.db", key, offsetIndex)
				if foundIndex {
					// Finaly we read the key value from the Data file, send it to the user and push it in the cache
					EI, cacheInfo := CheckData("Data/SSTable/SSTable"+fileNum+"/usertable-"+fileNum+"-Data.db", key, offsetData)
					cache.Add(key, *cacheInfo)
					return EI
				}
			}
		}
	}
	// If the element is not found in ANY SSTable we return nil
	return nil

}
