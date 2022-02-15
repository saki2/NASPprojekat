package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/bits"
	"os"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HyperLogLog struct {
	m   uint64
	p   uint8
	reg []uint8
}

func (hll *HyperLogLog) NewHyperLogLog(p uint8) {
	hll.p = p
	hll.m = uint64(math.Pow(2, float64(hll.p)))
	hll.reg = make([]uint8, hll.m)
}

func (hll *HyperLogLog) Add(elem string) {
	x := createHash(elem)
	k := 32 - hll.p
	r := leftmostActiveBit(x << hll.p)

	j := x >> uint(k)

	if r > hll.reg[j] {
		hll.reg[j] = r
	}

}

func leftmostActiveBit(x uint32) uint8 {
	return uint8(1 + bits.LeadingZeros32(x))
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum = sum + math.Pow(float64(-val), 2.0)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation < 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}


func createHash(stream string) uint32 {
	h := fnv.New32()
	h.Write([]byte(stream))
	sum := h.Sum32()
	h.Reset()
	return sum
}

func main() {
	hll := HyperLogLog{}
	hll.NewHyperLogLog(HLL_MAX_PRECISION)
	f, err := os.OpenFile("vezbe4/hyperloglog/testFile.txt", os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("open file error: %v", err)
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		hll.Add(sc.Text())
	}

	fmt.Println(hll.Estimate())
}

