package bloom_filter

import (
	"encoding/binary"
	"encoding/gob"
	"hash/fnv"
	"math"
	"os"
	"time"
)

const DEFAULT_FALSE_POSITIVE_RATE = 0.05

var FALSE_POSITIVE_RATE float64

type BloomFilter struct {
	M, K          uint     // Size of bloom filter and number of hash functions
	HashFunctions []uint32 // array of hashes
	Data          []byte   // Data array
}

func SetDefaultParam() {
	FALSE_POSITIVE_RATE = DEFAULT_FALSE_POSITIVE_RATE
}

func Panic(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func hash(s string) uint32 {
	// Hashes string to an unsigned 32 bit integer
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	Panic(err)
	return h.Sum32()
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	// Used to calculate how large the data segment of bloom filter should be
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	// Used to calculate how many hash functions should be used
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint) []uint32 {
	/* Because of difficulties with encoding the Bloom Filter structure
	with a hash.Hash32 type argument, a change has been made. The hashing is
	now done as following:
	- The current time is taken
	- hashed
	- appended to array of hashes
	- When hashing: from each individual hash in the array, the hash of the key is subtracted
	*/
	var h []uint32
	ts := uint32(time.Now().Unix())
	hashed := fnv.New32a()
	for i := uint(0); i < k; i++ {
		hashed.Reset()
		b := make([]byte, 8)
		binary.LittleEndian.PutUint32(b, ts+uint32(i))
		_, err := hashed.Write(b)
		Panic(err)
		h = append(h, hashed.Sum32())
	}
	return h
}

func (bf *BloomFilter) InitializeBloomFilter(expectedElements int, falsePositiveRate float64) bool {
	bf.M = CalculateM(expectedElements, falsePositiveRate)
	bf.K = CalculateK(expectedElements, bf.M)
	bf.HashFunctions = CreateHashFunctions(bf.K)
	bf.Data = make([]byte, bf.M)
	return true
}

func (bf *BloomFilter) AddElementBF(key string) bool {
	k := bf.K
	m := bf.M
	for i := uint(0); i < k; i++ {
		hashI := bf.HashFunctions[i]
		sum := math.Abs(float64(hashI - hash(key)))
		index := uint(sum) % m
		bf.Data[index] = 1
	}
	return true
}

func (bf *BloomFilter) Contains(key string) bool {
	contains := true
	k := bf.K
	m := bf.M
	for i := uint(0); i < k; i++ {
		hashI := bf.HashFunctions[i]
		sum := math.Abs(float64(hashI - hash(key)))
		index := uint(sum) % m
		if bf.Data[index] == 0 {
			contains = false
			break
		}
	}
	return contains

}

func ReadBloomFilter(path string) *BloomFilter {
	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	decoder := gob.NewDecoder(file)
	var bf = new(BloomFilter)
	file.Seek(0, 0)
	err = decoder.Decode(bf)
	Panic(err)
	return bf
}

func WriteBloomFilter(bf *BloomFilter, path string, createdFile *os.File) bool {
	// Function that either takes a reference to an already created file or a path where the file
	// will be opened/created
	var file *os.File
	var err error

	if path == "" {
		file = createdFile
	} else {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			// If selected file does not exist a new file is created
			file, err = os.Create(path)
			Panic(err)
		} else {
			// If it exists it is opened
			file, err = os.OpenFile(path, os.O_WRONLY, 0700)
			Panic(err)
		}
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bf)
	Panic(err)
	return true
}
