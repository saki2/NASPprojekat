package main

import "C"
import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type CountMinSketch struct {
	k uint
	m uint
	hashFunctions []hash.Hash32
	table [][]uint
}

func (c *CountMinSketch) newCountMinSketch() {
	c.m = CalculateM(0.01)
	c.k = CalculateK(0.01)
	c.hashFunctions = CreateHashFunctions(c.k)
	c.table = make([][] uint, c.k)
	for i := range c.table {
		c.table[i] = make([]uint, c.m)
	}
}

func (c *CountMinSketch) append(elem string) {
	for i:=0; i < int(c.k); i++ {
		c.hashFunctions[i].Reset()
		c.hashFunctions[i].Write([]byte(elem))
		j := c.hashFunctions[i].Sum32() % uint32(c.m)
		c.table[i][j] += 1
	}

}

func (c *CountMinSketch) frequency(elem string) uint {
	R := make([]uint, c.k, c.k)

	for i:=0; i < int(c.k); i++ {
		c.hashFunctions[i].Reset()
		c.hashFunctions[i].Write([]byte(elem))
		j := c.hashFunctions[i].Sum32() % uint32(c.m)
		R[i] = c.table[i][j]
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

func CreateHashFunctions(k uint) []hash.Hash32 {
	h := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h
}


