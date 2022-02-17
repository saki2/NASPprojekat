package memtable

import (
"fmt"
"math/rand"
)

var emptyString = ""
const (
	MAX_HEIGHT = 10
)

type Pair struct {
	Key 	string
	Value 	[]byte
}

type SkipList struct {
	MaxHeight int
	height int
	Size   int
	head   *Node
	Capacity uint64
}

type Node struct {
	// Elementi su predstavljeni pomocu cvorova
	// Cvor cuva kljuc, vrednost, tombstone i reference na ostale elemente liste
	// Cvor se u listi pozicionira na osnovu vrednosti kljuca
	Key   string
	Value []byte
	Tombstone bool
	next  []*Node
}

func (n *Node) newNode(key *string, value *[]byte, level int) {
	n.Key = *key
	n.Value = *value
	n.next = make([]*Node, level+1, level+1)
	n.Tombstone = false
}

func (s *SkipList) NewSkipList() {
	head := Node{}
	l := []byte(emptyString)
	head.newNode(&emptyString, &l, 0)
	s.head = &head
	s.MaxHeight = MAX_HEIGHT	// Takodje specificirano kroz konfiguracioni fajl
	s.height = 0		// Najveca visina koju lista trenutno poseduje
}

func (s *SkipList) SetMaxHeight(h int) {
	s.MaxHeight = h
}

func (s *SkipList) SetCapacity(c uint64) {
	s.Capacity = c
}

// Insert : Dodavanje elementa u skiplistu
func (s *SkipList) Insert(key string, value []byte) {
	update := make([]*Node, s.MaxHeight+1)
	current := s.head
	// Prolazak kroz skip listu da pronadjemo kljuc ili mesto gde treba da se ubaci
	for i:=s.height ; i >= 0; i-- {
		for current.next[i]!= nil && current.next[i].Key < key {
			current = current.next[i]
		}
		update[i] = current
	}
	current = current.next[0]

	// Element nije nadjen po kljucu i moze da se ubaci
	if current == nil || current.Key != key {
		newLevel := s.roll()
		if newLevel > s.height{
			for i:=s.height+1 ; i <= newLevel ; i++ {
				s.head.next = append(s.head.next, nil)
				update[i] = s.head
			}
			s.height = newLevel
		}

		newNode := Node{}
		newNode.newNode(&key, &value, newLevel)

		for i:=0 ; i<=newLevel ; i++ {
			// Azuriranje referenci
			newNode.next[i] = update[i].next[i]
			update[i].next[i] = &newNode
		}

		s.Size += 1

	}

}

// Find : Trazenje elementa u listi po vrednosti kljuca
// Vraca vrednost koja se nalazi pod tim kljucem
func (s *SkipList) Find(key string) []byte {

	update := make([]*Node, s.MaxHeight+1)
	current := s.head
	// Prolazak kroz skip listu da pronadjemo kljuc
	for i:=s.height ; i >= 0; i-- {
		for current.next[i]!= nil && current.next[i].Key < key {
			current = current.next[i]
		}
		update[i] = current
	}
	current = current.next[0]
	if current != nil && current.Key == key && current.Tombstone == false {
		return current.Value
	}
	return nil
}

// Delete : Logicko brisanje
// Ukoliko je element nadjen u listi pod zadatim kljucem tombstone se postavlja na true
func (s *SkipList) Delete(key string) bool {

	update := make([]*Node, s.MaxHeight+1)
	current := s.head
	// Prolazak kroz skip listu da pronadjemo kljuc
	for i:=s.height ; i >= 0; i-- {
		for current.next[i]!= nil && current.next[i].Key < key {
			current = current.next[i]
		}
		update[i] = current
	}
	current = current.next[0]
	if current != nil && current.Key == key && current.Tombstone == false {
		current.Tombstone = true
		return true
	}
	return false
}


// ExtractData : Vraca listu referenci na parove kljuc-vrednost
func (s *SkipList) ExtractData() []*Pair {
	h := s.head
	list := []*Pair{}

	for h.next[0] != nil {
		if h.next[0].Tombstone == false {
			p := Pair{h.next[0].Key, h.next[0].Value}
			list = append(list, &p)
		}
		h = h.next[0]
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
	for i:=0; i<=s.height ; i++ {
		fmt.Println("nivo", i)
		node := s.head.next[i]
		for node!=nil{
			fmt.Println(node.Key)
			node = node.next[i]
		}
	}
}