package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"project/structures/SSTable"
	"project/structures/memtable"
	"strconv"
	"strings"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value 0 - append 1 - deleted
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE

	DEFAULT_SEGMENT_SIZE = 100
)

var SEGMENT_SIZE uint64

func SetDefaultParam() {
	SEGMENT_SIZE = uint64(DEFAULT_SEGMENT_SIZE)
}


func  CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)

}

type WalSegment struct {
	NumElements int
}


func Append(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	err = file.Truncate(currentLen + int64(len(data))) // povecanje fajla za duzinu podatka
	if err != nil { return err }
	//mmapf, err := mmap.MapRegion(file, int(currentLen)+len(data), mmap.RDWR, 0, 0)
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[currentLen:], data) // upsuejmo sadrzaj iz podatka na disk u opsegu [currentLen:]
	mmapf.Flush()
	return nil
}

// Map maps an entire file into memory

// prot argument
// mmap.RDONLY - Maps the memory read-only. Attempts to write to the MMap object will result in undefined behavior.
// mmap.RDWR - Maps the memory as read-write. Writes to the MMap object will update the underlying file.
// mmap.COPY - Writes to the MMap object will affect memory, but the underlying file will remain unchanged.
// mmap.EXEC - The mapped memory is marked as executable.

// flag argument
// mmap.ANON - The mapped memory will not be backed by a file. If ANON is set in flags, f is ignored.
func Read(fileName string ) ([]byte, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE, 0644)
	fatal(err)
	defer file.Close()
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()     // brisanje segmenta (oslobadjanje memorije na kraju)
	result := make([]byte, len(mmapf))  // kopiranje sadrzaja
	copy(result, mmapf)
	return result, nil
}

func readRange(startIndex, endIndex int, fileName string) ([]byte, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE, 0644)
	fatal(err)
	defer file.Close()
	if startIndex < 0 || endIndex < 0 || startIndex > endIndex {
		return nil, errors.New("indices invalid")
	}
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()
	if startIndex >= len(mmapf) || endIndex > len(mmapf) {
		return nil, errors.New("indices invalid")
	}
	result := make([]byte, endIndex-startIndex)
	copy(result, mmapf[startIndex:endIndex])
	return result, nil
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}


func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func Add(key string, value []byte, fileName string, ts bool) error {
	f, err := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE, 0644)
	fatal(err)
	defer f.Close()

	crc := CRC32(value)

	crc_final := make([]byte, 4)
	binary.LittleEndian.PutUint32(crc_final, crc)


	Time := time.Now().Unix()
	timeStamp := make([]byte, 16)
	binary.LittleEndian.PutUint64(timeStamp, uint64(Time))

	tombstone := make([]byte, 8)
	if ts == false {
		binary.LittleEndian.PutUint64(tombstone, uint64(0))
	} else {
		binary.LittleEndian.PutUint64(tombstone, uint64(1))
	}

	keyLine := make([]byte, KEY_SIZE)
	binary.LittleEndian.PutUint64(keyLine, uint64(len([]byte(key))))

	valueLine := make([]byte, VALUE_SIZE)
	binary.LittleEndian.PutUint64(valueLine, uint64(len(value)))

	temp := make([]byte, 0)

	temp = append(temp, crc_final...)
	temp = append(temp, timeStamp...)
	temp = append(temp, tombstone...)
	temp = append(temp, keyLine...)
	temp = append(temp, valueLine...)
	temp = append(temp, []byte(key)...)
	temp = append(temp, value...)

	err = Append(f, temp)
	return err

}

// ScanWal : Gathers data from log segments to load in to memtable, returns path to last log segment
func ScanWal(memtableInstance *memtable.SkipList) string {
	files, err := ioutil.ReadDir("./Wal")
	SSTable.Panic(err)
	m := make(map[int]string)
	for _, file := range files {
		fileName := file.Name()
		tokens := strings.Split(fileName, "_")
		labels := strings.Split(tokens[1], ".")
		num, _ := strconv.Atoi(labels[0])
		m[num] = fileName
	}
	if len(m) > 0 {
		segmentCounter := 0 			// Memtable consists of data from these segments -- they are not to be deleted as they're not flushed yet
		for i := 1; i <= len(m); i++ {		// Read data from all log segments, add to memtable
			ReadData("./Wal/"+m[i], memtableInstance, &segmentCounter)
		}
		lowWatermark := len(files) - segmentCounter // Index up to which segments are deleted
		// Last segmentCounter segments are still active
		if lowWatermark > 0 {
			for i := 1; i <= lowWatermark; i++ {
				err := os.Remove("./Wal/" + m[i])
				SSTable.Panic(err)
			}
		}
		path := "./Wal/wal_" + strconv.Itoa(len(files)) + ".db"
		return path
	}
	return ""
}

// ReadData : Reads from a wal segment to insert to memtable
func ReadData(path string, memtableInstance *memtable.SkipList, segmentCounter *int) {
	//+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
	//|    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
	//+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+

	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	SSTable.Panic(err)
	defer file.Close()
	_, err = file.Seek(0, 0)
	SSTable.Panic(err)
	br := bufio.NewReader(file)

	for err == nil {
		crc := make([]byte, 4)
		_, err = br.Read(crc)
		if err != nil {
			break
		}
		timeStamp := make([]byte, 16)
		_, err = br.Read(timeStamp)
		t := binary.LittleEndian.Uint64(timeStamp)
		timestamp := int64(t)
		if err != nil {
			break
		}
		tombstone := make([]byte, 8)
		_, err = br.Read(tombstone)
		if err != nil {
			break
		}
		keySize := make([]byte, KEY_SIZE)
		_, err = br.Read(keySize)
		if err != nil {
			break
		}
		valueSize := make([]byte, VALUE_SIZE)
		_, err = br.Read(valueSize)
		if err != nil {
			break
		}
		key := make([]byte, binary.LittleEndian.Uint64(keySize))
		_, err = br.Read(key)
		value := make([]byte, binary.LittleEndian.Uint64(valueSize))
		_, err = br.Read(value)
		forFlush := memtableInstance.Insert(string(key), value, timestamp)

		if tombstone[0] == 1 {
			memtableInstance.Delete(string(key))
		}
		if forFlush != nil { 				// Memtable up to capacity, flush to disk
			SSTable.Flush(forFlush)
			memtableInstance.NewSkipList() 	// Reset memtable
		}
	}

	if memtableInstance.Size > 0 {	// Segment still has active data in the memtable
		*segmentCounter += 1
	} else {						// All previous segments including this one had data flushed - memtable is empty
		*segmentCounter = 0
	}

}

func DeleteWal() {
	files, err := ioutil.ReadDir("./Wal")
	SSTable.Panic(err)
	for _, file := range files {
		err = os.Remove("Wal/" + file.Name())
		SSTable.Panic(err)
	}
}

func CalculateSegmentSize(filename string) int {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0700)
	SSTable.Panic(err)
	defer file.Close()
	SSTable.Panic(err)
	br := bufio.NewReader(file)

	segmentSize := 0

	for err == nil {
		crc := make([]byte, 4)
		_, err = br.Read(crc)
		if err != nil {
			break
		}
		timeStamp := make([]byte, 16)
		_, err = br.Read(timeStamp)
		if err != nil {
			break
		}
		tombstone := make([]byte, 8)
		_, err = br.Read(tombstone)
		if err != nil {
			break
		}
		keySize := make([]byte, KEY_SIZE)
		_, err = br.Read(keySize)
		if err != nil {
			break
		}
		valueSize := make([]byte, VALUE_SIZE)
		_, err = br.Read(valueSize)
		if err != nil {
			break
		}
		key := make([]byte, binary.LittleEndian.Uint64(keySize))
		_, err = br.Read(key)
		value := make([]byte, binary.LittleEndian.Uint64(valueSize))
		_, err = br.Read(value)
		segmentSize += 1
	}
	return segmentSize
}
