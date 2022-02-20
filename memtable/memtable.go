package memtable

import (
	"encoding/binary"
	"fmt"
	"math/rand"
)

var (
	emptyString = ""
	MAX_HEIGHT int
	CAPACITY uint64
)

const (
	DEFAULT_MAX_HEIGHT = 10
	DEFAULT_CAPACITY = 100
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

func (n *Node) newNode(key *string, value *[]byte, level int, timestamp int64) {
	n.Key = *key
	n.Value = *value
	timeStampBin := make([]byte, 16)
	binary.LittleEndian.PutUint64(timeStampBin, uint64(timestamp))
	n.TimeStamp = timeStampBin
	n.Next = make([]*Node, level+1, level+1)
	n.Tombstone = false
}

func SetDefaultParam() {
	CAPACITY = DEFAULT_CAPACITY
	MAX_HEIGHT = DEFAULT_MAX_HEIGHT
}

func (s *SkipList) NewSkipList() {
	Head := Node{}
	l := []byte(emptyString)
	Head.newNode(&emptyString, &l, 0, 0)
	s.Head = &Head
	s.MaxHeight = MAX_HEIGHT
	s.height = 0
	s.Capacity = CAPACITY
	s.Size = 0
}

func (s *SkipList) SetMaxHeight(h int) {
	s.MaxHeight = h
}

func (s *SkipList) SetCapacity(c uint64) {
	s.Capacity = c
}

// Insert : Adding or updating element
// Returns skiplist to be flushed on disk when at capacity
func (s *SkipList) Insert(key string, value []byte, timestamp int64) *SkipList {
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
		newNode.newNode(&key, &value, newLevel, timestamp)

		// Updating references
		for i := 0; i <= newLevel; i++ {
			newNode.Next[i] = update[i].Next[i]
			update[i].Next[i] = &newNode
		}

		s.Size += 1
		// If max capacity is reached, skiplist is returned to be flushed on to the disk
		if uint64(s.Size) >= s.Capacity {
			return s
			//
		}

	}

	// Element found by key, to be updated
	if current != nil && current.Key == key {
		current.Value = value
		timeStamp := make([]byte, 16)
		binary.LittleEndian.PutUint64(timeStamp, uint64(timestamp))
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

// FindNode: The same as Find but insted of the value of Node it returns the whole Node
// It also returns the Node even if the Tombstone is true
func (s *SkipList) FindNode(key string) *Node {

	update := make([]*Node, s.MaxHeight+1)
	current := s.Head
	// Prolazak kroz skip listu da pronadjemo kljuc
	for i := s.height; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Key < key {
			current = current.Next[i]
		}
		update[i] = current
	}
	current = current.Next[0]
	if current != nil && current.Key == key {
		return current
	}
	return nil
}

// Delete : Logical, if element exists by key tombstone is set to true
func (s *SkipList) Delete(key string) bool {

	update := make([]*Node, s.MaxHeight+1)
	current := s.Head
	// Prolazak kroz skip listu da pronadjemo kljuc
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

// ExtractData : Vraca listu referenci na parove kljuc-vrednost
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
