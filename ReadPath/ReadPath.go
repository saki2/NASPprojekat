package ReadPath

import (
	"encoding/binary"
	"hash/crc32"
	"io/ioutil"
	bloom_filter "project/structures/Bloom_Filter"
	"project/structures/SSTable"
)

func Panic(err error) {
	panic(err.Error())
}

type ElementInfo struct {
	CRC       uint32
	Timestamp uint32
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func ReadPath(key string) {
	files, err := ioutil.ReadDir("./Data/SSTable")
	Panic(err)
	for _, file := range files {
		fileNum := file.Name()[7:]
		bf := bloom_filter.ReadBloomFilter("Data/SSTable/SSTable" + fileNum + "/usertable-" + fileNum + "-Filter.db")
		if bf.Contains(key) {
			offset1 := SSTable.ReadSummary("Data/SSTable/SSTable"+fileNum+"/usertable-"+fileNum+"-Summary.db", key)
			if offset1 != -1 {
				offset2 := SSTable.ReadIndex("Data/SSTable/SSTable"+fileNum+"/usertable-"+fileNum+"-Index.db", key, offset1)
				if offset2 != -1 {
					crc, timeStamp, tombStone, keySize, valueSize, currentKey, value := SSTable.ReadData("Data/SSTable/SSTable"+fileNum+"/usertable-"+fileNum+"-Data.db", key, offset2)
					if binary.LittleEndian.Uint32(crc) != crc32.ChecksumIEEE(value) {
						panic("Data corrupted")
					} else {
						EI := ElementInfo{}
						EI.CRC = binary.LittleEndian.Uint32(crc)
						EI.Timestamp = binary.LittleEndian.Uint32(timeStamp)
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
					}
				}
			}
		}
	}
}
