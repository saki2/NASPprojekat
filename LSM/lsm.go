package LSM

import (
	"bufio"
	"encoding/binary"
	"io/ioutil"
	"os"
	bloom_filter "project/structures/Bloom_Filter"
	"project/structures/SSTable"
	"project/structures/merkle"
	"strconv"
)

const MAX_LEVEL = 5

func Panic(err error) {
	if err != nil {
		panic(err.Error())
	}
}

type LSM struct {
}

func Compactions() {
	// We run through all the levels of the lsm tree
	// We start with merging the lowest levels and move our way up the tree
	for i := 1; i < MAX_LEVEL; i++ {
		// If MAX_LEVEL = 5, the files we can merge are in the folders: Level1, Level2, Level3, Level4
		files, err := ioutil.ReadDir("./Data/SSTable/Level" + strconv.Itoa(i))
		Panic(err)
		if len(files) <= 1 {
			// If there are no files to be merged than we exit the loop
			break
		}
		// We go through all the pairs found in the level directory
		x := 0 //  we pick the first two files than the next two and so on
		y := 1
		for j := 0; j < (len(files) / 2); j++ {
			filepath1 := "./Data/SSTable/Level" + strconv.Itoa(i) + "/" + files[x].Name() + "/usertable-" + strconv.Itoa(i) + "-Data.db"
			// We only need their Data file
			filepath2 := "./Data/SSTable/Level" + strconv.Itoa(i) + "/" + files[y].Name() + "/usertable-" + strconv.Itoa(i) + "-Data.db"
			// Starting the process of merging
			Merge(filepath1, filepath2, i+1)
			// When finished we discard the lower level files we no longer need
			err = os.RemoveAll("./Data/SSTable/Level" + strconv.Itoa(i) + "/" + files[x].Name())
			Panic(err)
			err = os.RemoveAll("./Data/SSTable/Level" + strconv.Itoa(i) + "/" + files[y].Name())
			Panic(err)
			x += 2
			y += 2
		}
	}

}

func Merge(sstable1Path string, sstable2Path string, level int) {
	// Opens the necessary files
	sstable1, err := os.OpenFile(sstable1Path, os.O_RDONLY, 0700)
	Panic(err)
	defer sstable1.Close()
	sstable2, err := os.OpenFile(sstable2Path, os.O_RDONLY, 0700)
	Panic(err)
	defer sstable2.Close()
	// Create the new files
	newSSTableName := SSTable.CreateSSTable(level)
	data, index, TOC, filter, metaData, summary := SSTable.CreateFilesOfSSTable(newSSTableName, level)
	defer data.Close()
	defer index.Close()
	defer TOC.Close()
	defer filter.Close()
	defer metaData.Close()
	defer summary.Close()
	SSTable.CreateTOC(level, TOC)

	// We approximate the number of keys found in the new file.
	// Each key in the Data file also has +37 bytes of additional info disregarding the size of the key and value
	// We divide the sum of number of bytes in each file and divide it by 37 resulting in an approximation of the maximal number of keys
	fileInfo1, err := sstable1.Stat()
	Panic(err)
	fileInfo2, err := sstable2.Stat()
	Panic(err)
	approximatedSize := (fileInfo1.Size()+fileInfo2.Size())/37 + 1

	// Creating the bloom filter
	bloomFilter := bloom_filter.BloomFilter{}
	bloomFilter.InitializeBloomFilter(int(approximatedSize), bloom_filter.FALSE_POSITIVE_RATE)

	// Creating an array of hashes to be put in the merkle tree
	hashVal := make([][20]byte, approximatedSize)

	// Creating the summary structure to be written in the summary file
	summaryStruct := SSTable.Summary{}
	summaryStruct.Elements = make(map[string]int)

	offsetData := 0
	offsetIndex := 0

	sstable1.Seek(0, 0)
	br1 := bufio.NewReader(sstable1)
	sstable2.Seek(0, 0)
	br2 := bufio.NewReader(sstable2)

	// We go through all the elements of Data1 and Data2 file
	IterateElements(br1, br2, data, index, &bloomFilter, &hashVal, &summaryStruct, &offsetData, &offsetIndex)
	// When all the files are created we check what the first key of the Data file is so we can put it in the Summary
	data.Seek(0, 0)
	br3 := bufio.NewReader(data)
	_, _, _, _, _, key, _ := ReadElement(br3)
	summaryStruct.FirstKey = string(key)

	Root := merkle.BuildTreeLeaf(hashVal)
	merkleTree := merkle.MerkleRoot{Root: Root}
	merkle.PreorderRecursive(merkleTree.Root, metaData)

	bloom_filter.WriteBloomFilter(&bloomFilter, "", filter) // Writing the bloom filter
	SSTable.WriteSummary(&summaryStruct, summary)           // Writing the summary
}

func IterateElements(br1, br2 *bufio.Reader, data, index *os.File, bloomFilter *bloom_filter.BloomFilter, hashVal *[][20]byte, summaryStruct *SSTable.Summary, offsetData, offsetIndex *int) {
	i := 0
	var err error
	var crc1, timeStamp1, tombStone1, keySize1, valueSize1, key1, value1 []byte
	var crc2, timeStamp2, tombStone2, keySize2, valueSize2, key2, value2 []byte
	var safeKey string

	// The first initial elements to be compared
	crc1, timeStamp1, tombStone1, keySize1, valueSize1, key1, value1 = ReadElement(br1)
	crc2, timeStamp2, tombStone2, keySize2, valueSize2, key2, value2 = ReadElement(br2)
	for err == nil {
		if string(key1) < string(key2) {
			WriteElement(crc1, timeStamp1, tombStone1, keySize1, valueSize1, key1, value1, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i)
			i++
			// advancing in the file
			crc1, timeStamp1, tombStone1, keySize1, valueSize1, key1, value1 = ReadElement(br1)
			// If we have reached the end of Data1 file we write the rest of the contents of Data2
			if crc1 == nil {
				WriteElement(crc2, timeStamp2, tombStone2, keySize2, valueSize2, key2, value2, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i)
				i++

				Finish(br2, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i, string(key2))
				break
			}
		} else if string(key1) > string(key2) {
			// If element of Data2 is smaller than element of Data1 than that element is written and Data2 is advanced while Data1 remains same
			WriteElement(crc2, timeStamp2, tombStone2, keySize2, valueSize2, key2, value2, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i)
			i++

			crc2, timeStamp2, tombStone2, keySize2, valueSize2, key2, value2 = ReadElement(br2)
			if crc2 == nil {
				if tombStone1[0] == 0 {
					WriteElement(crc1, timeStamp1, tombStone1, keySize1, valueSize1, key1, value1, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i)
					i++
				}
				Finish(br1, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i, string(key1))
				break
			}
		} else {
			// if current element of Data1 and Data 2 have the same key, the element with the bigger timestamp is chosen and written
			if binary.LittleEndian.Uint32(timeStamp1) > binary.LittleEndian.Uint32(timeStamp2) {
				if tombStone1[0] == 0 {
					WriteElement(crc1, timeStamp1, tombStone1, keySize1, valueSize1, key1, value1, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i)
					safeKey = string(key1)
					i++
				}
			} else {
				if tombStone2[0] == 0 {
					WriteElement(crc2, timeStamp2, tombStone2, keySize2, valueSize2, key2, value2, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i)
					safeKey = string(key2)
					i++
				}
			}
			// Both files are advanced
			crc1, timeStamp1, tombStone1, keySize1, valueSize1, key1, value1 = ReadElement(br1)
			crc2, timeStamp2, tombStone2, keySize2, valueSize2, key2, value2 = ReadElement(br2)
			if crc1 == nil && crc2 != nil {
				// If we reached the end of Data1 file, rest of Data2 file is written
				if tombStone2[0] == 0 {
					WriteElement(crc2, timeStamp2, tombStone2, keySize2, valueSize2, key2, value2, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i)
					i++
				}
				Finish(br2, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i, string(key2))
				break
			} else if crc1 != nil && crc2 == nil {
				// If we reached the end of Data2 file, rest of Data1 file is written
				if tombStone1[0] == 0 {
					WriteElement(crc1, timeStamp1, tombStone1, keySize1, valueSize1, key1, value1, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i)
					i++
				}
				Finish(br1, data, index, bloomFilter, hashVal, summaryStruct, offsetData, offsetIndex, i, string(key1))
				break
			} else if crc1 == nil && crc2 == nil {
				// If we reached the end of Data1 file AND the end of Data2 file, the loop is finished and the last
				// element to be written in stored in the summary (safeKey)
				summaryStruct.LastKey = safeKey
				break
			}
		}
	}
}

func WriteElement(crc, timeStamp, tombStone, keySize, valueSize, key, value []byte, data, index *os.File, bloomFilter *bloom_filter.BloomFilter, hashVal *[][20]byte, summary *SSTable.Summary, dataOffset, indexOffset *int, i int) {
	// Write the element inside the Data file
	binData := CollectElement(crc, timeStamp, tombStone, keySize, valueSize, key, value)
	_, err := data.Write(binData)
	Panic(err)
	//Write the element inside the Index file
	binIndex := SSTable.IndexSegmentToBinary(string(key), *dataOffset)
	_, err = index.Write(binIndex)
	Panic(err)
	// Increase the dataOffset
	*dataOffset += len(binData)
	// Write the element in the Summary
	summary.Elements[string(key)] = *indexOffset
	// Increase the indexOffset
	*indexOffset += len(binIndex)
	(*hashVal)[i] = merkle.Hash(value)
	bloomFilter.AddElementBF(string(key))

}

func Finish(br *bufio.Reader, data, index *os.File, bloomFilter *bloom_filter.BloomFilter, hashVal *[][20]byte, summary *SSTable.Summary, dataOffset, indexOffset *int, i int, lasKey string) {
	var err error
	_, err = br.Peek(4)
	if err != nil {
		summary.LastKey = lasKey
	}

	for err == nil {
		crc, timeStamp, tombStone, keySize, valueSize, key, value := ReadElement(br)
		// When we reach the end of the file we break the loop
		if crc == nil {
			break
		}
		WriteElement(crc, timeStamp, tombStone, keySize, valueSize, key, value, data, index, bloomFilter, hashVal, summary, dataOffset, indexOffset, i)
		// We check if we reached the end of the file, if so we store the last key written in the summary
		_, err := br.Peek(4)
		if err != nil {
			summary.LastKey = string(key)
		}
	}
}

func CollectElement(crc, timeStamp, tombStone, keySize, valueSize, key, value []byte) []byte {
	size := binary.LittleEndian.Uint64(keySize) + binary.LittleEndian.Uint64(valueSize) + 37
	element := make([]byte, 0, size)
	element = append(element, crc...)
	element = append(element, timeStamp...)
	element = append(element, tombStone...)
	element = append(element, keySize...)
	element = append(element, valueSize...)
	element = append(element, key...)
	element = append(element, value...)
	return element
}

func ReadElement(br *bufio.Reader) ([]byte, []byte, []byte, []byte, []byte, []byte, []byte) {
	crc := make([]byte, 4)
	_, err := br.Read(crc)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil
	}
	timeStamp := make([]byte, 16)
	_, err = br.Read(timeStamp)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil
	}
	tombStone := make([]byte, 1)
	_, err = br.Read(tombStone)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil
	}
	keySize := make([]byte, 8)
	_, err = br.Read(keySize)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil
	}
	valueSize := make([]byte, 8)
	_, err = br.Read(valueSize)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil
	}
	key := make([]byte, binary.LittleEndian.Uint64(keySize))
	_, err = br.Read(key)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil
	}
	value := make([]byte, binary.LittleEndian.Uint64(valueSize))
	_, err = br.Read(value)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil
	}
	return crc, timeStamp, tombStone, keySize, valueSize, key, value
}
