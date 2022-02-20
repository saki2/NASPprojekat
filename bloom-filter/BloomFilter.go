package main

import (
	"crypto/sha1"
	"hash"
	"math"
	"time"
)

type BloomFilter struct {
	bytes []byte
	k uint
	m uint
	hashFunctions []hash.Hash
	set []int
}

func (b *BloomFilter) newBloomFilter() {
	b.m = CalculateM(100, 0.05)
	b.k = CalculateK(100, b.m)
	b.hashFunctions = CreateHashFunctions(b.k)
	b.set = make([]int, b.m, b.m)
}

func (b *BloomFilter) append(elem string) {

	for i:=0; i < len(b.hashFunctions); i++ {
		b.hashFunctions[i].Write([]byte(elem))
		ts := uint(time.Now().Unix())
		index := Sum32WithSeed([]byte (elem), uint32(ts)) % uint32(b.m)
		b.set[index] = 1

	}
}

func (b *BloomFilter) isContain(elem string) bool{
	for i:=0; i < len(b.hashFunctions); i++ {
		b.hashFunctions[i].Reset()
		b.hashFunctions[i].Write([]byte(elem))
		ts := uint(time.Now().Unix())
		index := Sum32WithSeed([]byte (elem), uint32(ts)) % uint32(b.m)
		if b.set[index] == 0 {
			return false
		}
	}
	return true
}



func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}



func CreateHashFunctions(k uint) []hash.Hash {
	var h []hash.Hash
	for i := uint(0); i < k; i++ {
		h = append(h, sha1.New())
	}
	return h
}
