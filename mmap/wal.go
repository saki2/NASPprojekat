package wal
import (
	"encoding/binary"
	"errors"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"log"
	"os"
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
)


func  CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)

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
func read(fileName string ) ([]byte, error) {
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

func Add(key string, value []byte, fileName string) {
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
	binary.LittleEndian.PutUint64(tombstone, uint64(0))

	keyLine := make([]byte, KEY_SIZE)
	binary.LittleEndian.PutUint64(keyLine, uint64(len([]byte(key))))

	valueLine := make([]byte, VALUE_SIZE)
	binary.LittleEndian.PutUint64(valueLine, uint64(len([]byte(key))))

	temp := make([]byte, 0)

	temp = append(temp, crc_final...)
	temp = append(temp, timeStamp...)
	temp = append(temp, tombstone...)
	temp = append(temp, keyLine...)
	temp = append(temp, valueLine...)
	temp = append(temp, []byte(key)...)
	temp = append(temp, value...)

	_ = Append(f, temp)
	f.Close()


}

//f, err := os.OpenFile("file", os.O_RDWR | os.O_CREATE, 0644)
//fatal(err)
//defer f.Close()
//
//t, _ := read(f)
// read crc from file
//fmt.Println(binary.LittleEndian.Uint32(t[:4]))
//// read time from file
//fmt.Println(binary.LittleEndian.Uint64(t[4:20]))
// ....















