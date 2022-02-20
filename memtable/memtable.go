package memtable

import (
	"encoding/binary"
	"fmt"
	"math/rand"
)

var emptyString = ""

const (
	MAX_HEIGHT = 10
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
	// Elementi su predstavljeni pomocu cvorova
	// Cvor cuva kljuc, vrednost, tombstone i reference na ostale elemente liste
	// Cvor se u listi pozicionira na osnovu vrednosti kljuca
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

func (s *SkipList) NewSkipList() {
	Head := Node{}
	l := []byte(emptyString)
	Head.newNode(&emptyString, &l, 0, 0)
	s.Head = &Head
	s.MaxHeight = MAX_HEIGHT // Takodje specificirano kroz konfiguracioni fajl
	s.height = 0             // Najveca visina koju lista trenutno poseduje
	s.Capacity = 5
}

func (s *SkipList) SetMaxHeight(h int) {
	s.MaxHeight = h
}

func (s *SkipList) SetCapacity(c uint64) {
	s.Capacity = c
}

// Insert : Dodavanje elementa u skiplistu
func (s *SkipList) Insert(key string, value []byte, timestamp int64) *SkipList {
	update := make([]*Node, s.MaxHeight+1)
	current := s.Head
	// Prolazak kroz skip listu da pronadjemo kljuc ili mesto gde treba da se ubaci
	for i := s.height; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Key < key {
			current = current.Next[i]
		}
		update[i] = current
	}
	current = current.Next[0]

	// Element nije nadjen po kljucu i moze da se ubaci
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

		for i := 0; i <= newLevel; i++ {
			// Azuriranje referenci
			newNode.Next[i] = update[i].Next[i]
			update[i].Next[i] = &newNode
		}

		s.Size += 1
		// Novo
		// If max capacity is reached we return the skiplist to be flushed on to the disk
		if uint64(s.Size) >= s.Capacity {
			return s
			//
		}

	}
	// Novo
	if current != nil && current.Key == key {
		fmt.Println(current.Key)
		fmt.Println(current.Value)
		fmt.Println(value)
		current.Value = value
		fmt.Println(current.Value)
		//
	}
	return nil

}

// Find : Trazenje elementa u listi po vrednosti kljuca
// Vraca vrednost koja se nalazi pod tim kljucem
func (s *SkipList) Find(key string) []byte {

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

// Delete : Logicko brisanje
// Ukoliko je element nadjen u listi pod zadatim kljucem tombstone se postavlja na true
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
	level := 0 // alwasy start from level 0

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
