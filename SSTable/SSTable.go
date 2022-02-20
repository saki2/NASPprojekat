package SSTable

import (
	"io/ioutil"
	"os"
	"project/structures/Bloom_Filter"
	"project/structures/memtable"
	"project/structures/merkle"
	"strconv"
)

//=====================================================================================================================
// Universal function

func Panic(err error) {
	if err != nil {
		panic(err.Error())
	}
}

//=====================================================================================================================
// SSTable
// a) Writing

func CreateSSTable() (string, int) {
	/* For each SSTable a new folder is created inside "Data/SSTable"
	 The numeric number of the new SSTable is calculated based on the
	 numeric number of the latest SSTable, for example:
	-Data
	--SSTable
	---SSTable1
	---SSTable2
	---SSTable3
	---SSTable4
	The next SSTable will be: SSTable5
	*/
	files, err := ioutil.ReadDir("./Data/SSTable")
	Panic(err)

	var newDirName string
	var newDirNumber int

	if len(files) == 0 {
		newDirName = "SSTable1"
		newDirNumber = 1
	} else {
		newDirNumber = len(files) + 1
		newDirName = "SSTable" + strconv.Itoa(newDirNumber)
	}
	//Create a directory in path : Project/Data/SSTable
	err = os.Mkdir("./Data/SSTable/"+newDirName, 0755)
	Panic(err)

	return newDirName, newDirNumber
}

func CreateFilesOfSSTable(SSTableDirName string, SSTableDirNumber int) (*os.File, *os.File, *os.File, *os.File, *os.File, *os.File) {
	/* Each SSTable folder will contain the next files:
	usertable-1-Data.db; usertable-1-Index.db; usertable-1-TOC.db; usertable-1-Filter.db; usertable-1-Metadata.db
	*/
	data, err := os.Create("./Data/SSTable/" + SSTableDirName + "/usertable-" + strconv.Itoa(SSTableDirNumber) + "-Data.db")
	Panic(err)
	index, err := os.Create("./Data/SSTable/" + SSTableDirName + "/usertable-" + strconv.Itoa(SSTableDirNumber) + "-Index.db")
	Panic(err)
	TOC, err := os.Create("./Data/SSTable/" + SSTableDirName + "/usertable-" + strconv.Itoa(SSTableDirNumber) + "-TOC.db")
	Panic(err)
	filter, err := os.Create("./Data/SSTable/" + SSTableDirName + "/usertable-" + strconv.Itoa(SSTableDirNumber) + "-Filter.db")
	Panic(err)
	metaData, err := os.Create("./Data/SSTable/" + SSTableDirName + "/usertable-" + strconv.Itoa(SSTableDirNumber) + "-Metadata.txt")
	Panic(err)
	summary, err := os.Create("./Data/SSTable/" + SSTableDirName + "/usertable-" + strconv.Itoa(SSTableDirNumber) + "-Summary.db")
	Panic(err)
	return data, index, TOC, filter, metaData, summary
}

func CreateTOC(SSTableDirNumber int, file *os.File) {
	toc := [6]string{"usertable-" + strconv.Itoa(SSTableDirNumber) + "-Data.db",
		"usertable-" + strconv.Itoa(SSTableDirNumber) + "-Index.db",
		"usertable-" + strconv.Itoa(SSTableDirNumber) + "-TOC.db",
		"usertable-" + strconv.Itoa(SSTableDirNumber) + "-Filter.db",
		"usertable-" + strconv.Itoa(SSTableDirNumber) + "-Metadata.db",
		"usertable-" + strconv.Itoa(SSTableDirNumber) + "-Summary.db"}
	for _, eachFile := range toc {
		_, err := file.WriteString(eachFile + "\n")
		Panic(err)
	}

}

func Flush(s *memtable.SkipList) {
	newDirName, newDirNumber := CreateSSTable()
	data, index, TOC, filter, metaData, summary := CreateFilesOfSSTable(newDirName, newDirNumber)
	defer data.Close()
	defer index.Close()
	defer TOC.Close()
	defer filter.Close()
	defer metaData.Close()
	defer summary.Close()

	// Creating the TOC
	CreateTOC(newDirNumber, TOC)

	// Initializing the Bloom Filter
	bloomFilter := bloom_filter.BloomFilter{}
	bloomFilter.InitializeBloomFilter(s.Size, bloom_filter.FALSE_POSITIVE_RATE)

	dataOffset := 0
	indexOffset := 0

	// Creating empty Summary to be written in the Summary file
	summaryStruct := Summary{}
	summaryStruct.elements = make(map[string]int)

	// Get the current element to be written in SSTable
	node := s.Head.Next[0]

	// Writing the first element of the index into the summary

	summaryStruct.firstKey = node.Key

	// Creating an array of values to be put in the merkle tree
	hashVal := make([][20]byte, s.Capacity)
	i := 0 // index of hashVal

	for node != nil {
		// Turn the element into a binary array and write it into the Data file
		binData := DataSegmentToBinary(node)
		_, err := data.Write(binData)
		Panic(err)

		bloomFilter.AddElementBF(node.Key)

		binIndex := IndexSegmentToBinary(node.Key, dataOffset)
		_, err = index.Write(binIndex)
		Panic(err)
		// After we write the element into the data segment, we increase the data offset by its size
		dataOffset += len(binData)

		summaryStruct.elements[node.Key] = indexOffset
		// After we write the element into the Index segment, we increase the index offset by its size
		indexOffset += len(binIndex)

		hashVal[i] = merkle.Hash(node.Value)
		i++

		nodeNext := node.Next[0]
		// Writing the last element of the index into the summary
		if nodeNext == nil {
			summaryStruct.lastKey = node.Key
		}
		node = nodeNext
	}
	// Writing the metadata
	Root := merkle.BuildTreeLeaf(hashVal)
	merkleTree := merkle.MerkleRoot{Root: Root}
	merkle.PreorderRecursive(merkleTree.Root, metaData)

	bloom_filter.WriteBloomFilter(&bloomFilter, "", filter) // Writing the bloom filter
	WriteSummary(&summaryStruct, summary)                   // Writing the summary
}
