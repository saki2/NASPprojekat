package memtable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var emptyString = ""

const (
	MAX_HEIGHT = 10
	CAPACITY = 1000
)

type Pair struct {
	Key   string
	Value []byte
}

type SkipList struct {
	MaxHeight int
	height    int
	Size      int
	Head      *Node
	Capacity  uint64
}

type Node struct {
	// Elements are represented by nodes
	Key       string
	Value     []byte
	TimeStamp []byte
	Tombstone bool
	Next      []*Node
}

func (n *Node) newNode(key *string, value *[]byte, level int) {
	n.Key = *key
	n.Value = *value
	Time := time.Now().Unix()
	timeStamp := make([]byte, 16)
	binary.LittleEndian.PutUint64(timeStamp, uint64(Time))
	n.TimeStamp = timeStamp
	n.Next = make([]*Node, level+1, level+1)
	n.Tombstone = false
}

func (s *SkipList) NewSkipList() {
	Head := Node{}
	l := []byte(emptyString)
	Head.newNode(&emptyString, &l, 0)
	s.Head = &Head
	s.loadConfig()
	s.height = 0
}

func (s *SkipList) loadConfig() {
	file, err := os.OpenFile("memtable/config.txt", os.O_RDONLY, 0700)
	if errors.Is(err, os.ErrNotExist) {
		s.Capacity = CAPACITY
		s.MaxHeight = MAX_HEIGHT
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()
	line := scanner.Text()
	split := strings.Split(line, "=")
	c, err := strconv.Atoi(split[1])
	if err != nil {
		s.Capacity = uint64(c)
	} else {
		// Config file format incorrect, memtable Capacity set to default
		s.Capacity = CAPACITY
	}

	scanner.Scan()
	line = scanner.Text()
	split = strings.Split(line, "=")
	m, err := strconv.Atoi(split[1])
	if err == nil {
		s.MaxHeight = m
	} else {
		// Config file format incorrect, memtable MaxHeight set to default
		s.MaxHeight = MAX_HEIGHT
	}
}

// Insert : Adding or updating element
func (s *SkipList) Insert(key string, value []byte) *SkipList {
	update := make([]*Node, s.MaxHeight+1)
	current := s.Head

	for i := s.height; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Key < key {
			current = current.Next[i]
		}
		update[i] = current
	}
	current = current.Next[0]

	// Element not found by key, can be inserted
	if current == nil || current.Key != key {
		newLevel := s.roll()
		if newLevel > s.height {
			for i := s.height + 1; i <= newLevel; i++ {
				s.Head.Next = append(s.Head.Next, nil)
				update[i] = s.Head
			}
			s.height = newLevel
		}

		newNode := Node{}
		newNode.newNode(&key, &value, newLevel)

		// Updating references
		for i := 0; i <= newLevel; i++ {
			newNode.Next[i] = update[i].Next[i]
			update[i].Next[i] = &newNode
		}

		s.Size += 1

		// If max capacity is reached we return the skiplist to be flushed on to the disk
		if uint64(s.Size) >= s.Capacity {
			return s
			//
		}

	}

	// Element found by key, to be updated
	if current != nil && current.Key == key {
		current.Value = value
		Time := time.Now().Unix()
		timeStamp := make([]byte, 16)
		binary.LittleEndian.PutUint64(timeStamp, uint64(Time))
		current.TimeStamp = timeStamp
	}
	return nil

}

// Find : Returns value of the element found by key
func (s *SkipList) Find(key string) []byte {

	update := make([]*Node, s.MaxHeight+1)
	current := s.Head
	for i := s.height; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Key < key {
			current = current.Next[i]
		}
		update[i] = current
	}
	current = current.Next[0]
	if current != nil && current.Key == key && current.Tombstone == false {
		return current.Value
	}
	return nil
}

// Delete : Logical, if element exists by key tombstone is set to true
func (s *SkipList) Delete(key string) bool {

	update := make([]*Node, s.MaxHeight+1)
	current := s.Head
	for i := s.height; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Key < key {
			current = current.Next[i]
		}
		update[i] = current
	}
	current = current.Next[0]
	if current != nil && current.Key == key && current.Tombstone == false {
		current.Tombstone = true
		return true
	}
	return false
}

// ExtractData : Returns list of references to Pair objects
func (s *SkipList) ExtractData() []*Pair {
	h := s.Head
	list := []*Pair{}

	for h.Next[0] != nil {
		if h.Next[0].Tombstone == false {
			p := Pair{h.Next[0].Key, h.Next[0].Value}
			list = append(list, &p)
		}
		h = h.Next[0]
	}
	return list
}

func (s *SkipList) roll() int {
	level := 0 // always start from level 0

	// We roll until we don't get 1 from rand function and we did not
	// outgrow maxHeight. BUT rand can give us 0, and if that is the case
	// than we will just increase level, and wait for 1 from rand!
	for ; rand.Int31n(2) == 1; level++ {
		if level > s.height {
			// When we get 1 from rand function and we did not
			// outgrow maxHeight, that number becomes new height
			return level
		}
	}
	return level
}

func (s *SkipList) PrintList() {
	for i := 0; i <= s.height; i++ {
		fmt.Println("nivo", i)
		node := s.Head.Next[i]
		for node != nil {
			fmt.Println(node.Key)
			node = node.Next[i]
		}
	}
}

func (s *SkipList) PrintElements() {
	node := s.Head.Next[0]
	for node != nil {
		data := binary.BigEndian.Uint64(node.TimeStamp)
		fmt.Println("Key: \"", node.Key, "\"; Value: \"", string(node.Value), "\"; TimeStamp: ", data)
		node = node.Next[0]
	}
}

func (s *SkipList) Flush() *SkipList {
	return s
}
