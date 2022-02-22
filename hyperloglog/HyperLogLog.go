package hyperloglog

import (
	"encoding/gob"
	"hash/fnv"
	"math"
	"math/bits"
	"os"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HyperLogLog struct {
	M   uint64
	P   uint8
	Reg []uint8
}

func (hll *HyperLogLog) NewHyperLogLog(p uint8) {
	hll.P = p
	hll.M = uint64(math.Pow(2, float64(hll.P)))
	hll.Reg = make([]uint8, hll.M)
}

func (hll *HyperLogLog) Add(elem string) {
	x := createHash(elem)
	k := 32 - hll.P
	r := leftmostActiveBit(x << hll.P)

	j := x >> uint(k)

	if r > hll.Reg[j] {
		hll.Reg[j] = r
	}

}

func leftmostActiveBit(x uint32) uint8 {
	return uint8(1 + bits.LeadingZeros32(x))
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum = sum + math.Pow(float64(-val), 2.0)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation < 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func Panic(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func ReadHLL(path string) *HyperLogLog {
	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	decoder := gob.NewDecoder(file)
	var hll = new(HyperLogLog)
	file.Seek(0, 0)
	err = decoder.Decode(hll)
	Panic(err)
	return hll
}

func WriteHLL(hll *HyperLogLog, path string, createdFile *os.File) {
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
	err = encoder.Encode(hll)
	Panic(err)	
}

func createHash(stream string) uint32 {
	h := fnv.New32()
	h.Write([]byte(stream))
	sum := h.Sum32()
	h.Reset()
	return sum
}

