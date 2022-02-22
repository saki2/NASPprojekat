package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"project/structures/CRUD"
	"project/structures/Initialization"
	"project/structures/ReadPath"
	"project/structures/TokenBucket"
	"project/structures/WritePath"
	"project/structures/lru"
	"project/structures/memtable"
	wal "project/structures/mmap"
	"strings"
	"time"
)

func ReadUserInput(mem *memtable.SkipList, cache *lru.Cache) {
	fmt.Println("Input the command you wish to be executed (c - create; r - read; u - update; d - delete)")
	fmt.Println(">> ")
	var crud string
	fmt.Scanln(&crud)
	var key string
	fmt.Println("Input the key: \n>> ")
	fmt.Scanln(&key)
	switch crud {
	case "c", "C":
		// WRITING
		var value string
		fmt.Println("Input the value: \n>>")
		fmt.Scanln(&value)
		CRUD.Create(mem, cache, key, []byte(value))
		fmt.Println("Successfully created an element ")
	case "u", "U":
		// Writing
		var value string
		fmt.Println("Input the value: \n>>")
		fmt.Scanln(&value)
		CRUD.Update(mem, cache, key, []byte(value))
		fmt.Println("Successfully updated an element ")
	case "d", "D":
		//Writing
		CRUD.Delete(mem, cache, key)
		fmt.Println("Successfully deleted an element ")
	case "r", "R":
		//Reading
		element := CRUD.Read(mem, cache, key)
		ReadPath.PrintElement(element)
	default:
		fmt.Println("Invalid command, try again")
	}
}

func ReadFileInput(path string, mem *memtable.SkipList, cache *lru.Cache) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, "|")
		function := split[0]
		key := split[1]
		value := split[2]
		if function == "c" {
			CRUD.Create(mem, cache, key, []byte(value))
		} else if function == "r" {
			element := ReadPath.ReadPath(mem, cache, key)
			ReadPath.PrintElement(element)
		} else if function == "u" {
			CRUD.Update(mem, cache, key, []byte(value))
		} else if function == "d" {
			CRUD.Delete(mem, cache, key)
		}
	}
	fmt.Println("Successfully read file")
}

func meni(mem *memtable.SkipList, cache *lru.Cache, tb *TokenBucket.TokenBucket) {
	var err error
	for err == nil {
		fmt.Println("Chose option: ")
		fmt.Println("1) Input path to file")
		fmt.Println("2) Input CRUD command")
		fmt.Println("3) Exit")
		fmt.Println(">> ")
		var choice string
		fmt.Scanln(&choice)
		if choice == "1" {
			fmt.Println("The file should be the following format: CRUD COMMAND|KEY|VALUE")
			fmt.Println("Example: d|Mango|/ ; c|Papaya|Orange")
			fmt.Println("Input the file path or X to return: \n>> ")
			var path string
			fmt.Scanln(&path)
			if path == "x" || path == "X"{
				continue
			}else {
				ReadFileInput(path, mem, cache)
			}
		} else if choice == "2" {
			// TOKEN BUCKET ALGORITHM
			if Now()-tb.LastReset >= tb.Interval {	// Interval has passed, counters are reset
				tb.LastReset = Now()
				tb.AvailableReq = tb.MaxReq
				ReadUserInput(mem, cache)
				tb.AvailableReq -= 1
			} else {	// In interval
				if tb.AvailableReq-1 > 0 {
					ReadUserInput(mem, cache)
					tb.AvailableReq -= 1
				} // else : Too many requests for the set time interval
			}
			ReadUserInput(mem, cache)
		} else if choice == "3" {
			os.Exit(3)
		} else {
			fmt.Println("Invalid option, try again")
			continue
		}
	}
}


func main() {

	Initialization.Configure()
	Initialization.CreateDataFiles()

	// Initializing structures in memory
	memtableInstance := memtable.SkipList{}
	memtableInstance.NewSkipList()
	cache := lru.NewCache()
	tb := TokenBucket.NewTokenBucket()
	tb.LastReset = Now()
	tb.AvailableReq = tb.MaxReq

	// Scanning wal directory
	WritePath.WalSegmentName = wal.ScanWal(&memtableInstance)
	if memtableInstance.Size == 0 {		// There is no leftover data from logs
		WritePath.CreateLogFile()
		WritePath.SegmentElements = 0
	} else {
		WritePath.SegmentElements = uint64(wal.CalculateSegmentSize(WritePath.WalSegmentName))
	}

	meni(&memtableInstance, cache, tb)
}

func Now() int64 {
	return time.Now().Unix()
}
