package SSTable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"project/structures/memtable"
)

//=====================================================================================================================
// Data

// Write

func DataSegmentToBinary(node *memtable.Node) []byte {
	//+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
	//|    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
	//+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+

	// Attributes that are not binary are changed to be byte arrays
	tombStone := []byte{0}
	if node.Tombstone {
		tombStone[0] = 1
	}
	key := []byte(node.Key)

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, uint64(len(key)))

	valueSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSize, uint64(len(node.Value)))

	size := binary.LittleEndian.Uint64(keySize) + binary.LittleEndian.Uint64(valueSize) + 37
	element := make([]byte, 0, size)

	crc := make([]byte, 4)
	tmp := crc32.ChecksumIEEE(node.Value)
	binary.LittleEndian.PutUint32(crc, tmp)
	// Collects all the data into one element that will be written in the SSTable
	element = append(element, crc...)
	element = append(element, node.TimeStamp...)
	element = append(element, tombStone...)
	element = append(element, keySize...)
	element = append(element, valueSize...)
	element = append(element, key...)
	element = append(element, node.Value...)

	return element
}

// Read

func ReadData(path string, key string, offset int64) ([]byte, []byte, []byte, []byte, []byte, []byte, []byte) {
	//+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
	//|    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
	//+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+

	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	_, err = file.Seek(offset, 0)
	Panic(err)
	br := bufio.NewReader(file)

	crc := make([]byte, 4)
	_, err = br.Read(crc)
	Panic(err)
	timeStamp := make([]byte, 16)
	_, err = br.Read(timeStamp)
	Panic(err)
	tombStone := make([]byte, 1)
	_, err = br.Read(tombStone)
	Panic(err)
	keySize := make([]byte, 8)
	_, err = br.Read(keySize)
	Panic(err)
	valueSize := make([]byte, 8)
	_, err = br.Read(valueSize)
	Panic(err)

	currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
	_, err = br.Read(currentKey)
	Panic(err)
	if key == string(currentKey) {
		value := make([]byte, binary.LittleEndian.Uint64(valueSize))
		_, err = br.Read(value)
		return crc, timeStamp, tombStone, keySize, valueSize, currentKey, value
	} else {
		panic("Error: Key not found in estimated position")
	}
}

// PrintData : Used for debugging, prints the contents of the Data file
func PrintData(path string) {

	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	_, err = file.Seek(0, 0)
	Panic(err)
	br := bufio.NewReader(file)

	i := 1
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
		tombStone := make([]byte, 1)
		_, err = br.Read(tombStone)
		if err != nil {
			break
		}
		keySize := make([]byte, 8)
		_, err = br.Read(keySize)
		if err != nil {
			break
		}
		valueSize := make([]byte, 8)
		_, err = br.Read(valueSize)
		if err != nil {
			break
		}
		currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
		_, err = br.Read(currentKey)
		if err != nil {
			break
		}
		value := make([]byte, binary.LittleEndian.Uint64(valueSize))
		_, err = br.Read(value)

		var ts string
		if tombStone[0] == 1 {
			ts = "True"
		} else {
			ts = "False"
		}

		fmt.Println(i, ". CRC: ", binary.LittleEndian.Uint32(crc),
			"; Timestamp: ", binary.LittleEndian.Uint32(timeStamp),
			"; Tombstone: ", ts,
			"; Key size: ", binary.LittleEndian.Uint64(keySize),
			"; Value Size: ", binary.LittleEndian.Uint64(valueSize),
			"; Key: ", string(currentKey),
			"; Value:", string(value))
		i++
	}
}
