package count_min_sketch

import "C"
import (
	"encoding/gob"
	"hash/fnv"
	"math"
	"os"
	"project/structures/Bloom_Filter"
)

type CountMinSketch struct {
	K uint
	M uint
	HashFunctions []uint32
	Table [][]uint
}

func (c *CountMinSketch) newCountMinSketch() {
	c.M = CalculateM(0.01)
	c.K = CalculateK(0.01)
	c.HashFunctions = bloom_filter.CreateHashFunctions(c.K)
	c.Table = make([][] uint, c.K)
	for i := range c.Table {
		c.Table[i] = make([]uint, c.M)
	}

}

func hash_(s string) uint32 {
	// Hashes string to an unsigned 32 bit integer
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		panic(err.Error())
	}
	return h.Sum32()
}

func (c *CountMinSketch) append(key string)  {
	for i:=0; i < int(c.K); i++ {
		hashI := c.HashFunctions[i]
		sum := math.Abs(float64(hashI - hash_(key)))
		j:= uint(sum) % c.M
		c.Table[i][j] += 1
	}
}

func (c *CountMinSketch) frequency(key string) uint {
	R := make([]uint, c.K, c.K)

	for i:=0; i < int(c.K); i++ {
		hashI := c.HashFunctions[i]
		sum := math.Abs(float64(hashI - hash_(key)))
		j:= uint(sum) % c.M
		R[i] = c.Table[i][j]
	}


	min := R[0]
	for k := 1; k < len(R); k++ {
		if R[k] < min {
			min = R[k]
		}
	}

	return min
}

func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func Panic(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func ReadCMS(path string) *CountMinSketch {

	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	decoder := gob.NewDecoder(file)
	var cms = new(CountMinSketch)
	file.Seek(0, 0)
	err = decoder.Decode(cms)
	Panic(err)
	return cms
}

func WriteCMS(cms *CountMinSketch, path string, createdFile *os.File) {
	var file *os.File
	var err error
	if path == "" {
		file = createdFile
	}else {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			file, err = os.Create(path)
			Panic(err)
		} else {
			file, err = os.OpenFile(path, os.O_WRONLY, 0700)
			Panic(err)
		}
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cms)
	Panic(err)
}
