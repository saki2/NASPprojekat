package skiplist

import (
	"fmt"
	"math/rand"
)

var emptyString = ""

type SkipList struct {
	MaxHeight int
	height int
	Size   int
	head   *Node
	MaxSize uint64
}

type Node struct {
	Key   string
	Value []byte
	Tombstone bool
	next  []*Node
}

func (n *Node) newNode(height int, key *string, value *[]byte) {
	n.Key = *key
	n.Value = *value
	n.next = make([]*Node, height, height)
	n.Tombstone = false
}

func (s *SkipList) NewSkipList() {
	head := Node{}
	l := []byte(emptyString)
	head.newNode(0, &emptyString, &l)
	s.head = &head
	s.MaxHeight = 0
}


func (s *SkipList) updateList(elem *string) []*Node {
	update := make([]*Node, s.MaxHeight, s.MaxHeight)
	x := s.head
	for i := s.MaxHeight - 1; i >= 0; i-- {
		for  x.next[i] != nil && x.next[i].Key < *elem {
			x = x.next[i]
		}
		update[i] = x
	}
	return update
}

func (s *SkipList) Find(elem string, update []*Node) *Node {

	if update == nil {
		update = s.updateList(&elem)
	}

	if len(update) > 0 {
		item := update[0].next[0]
		if item != nil && item.Key == elem && item.Tombstone == false {
			return item
		}
	}
	return nil
}

func (s *SkipList) Insert(key string, value []byte) {

	node := Node{}
	node.newNode(roll(), &key, &value)
	s.MaxHeight = max(s.MaxHeight, len(node.next))

	for len(s.head.next) <  len(node.next) {
		s.head.next = append(s.head.next, nil)
	}

	update := s.updateList(&key)
	if s.Find(key, update) == nil {
		for i:=0; i<len(node.next); i++ {
			node.next[i] = update[i].next[i]
			update[i].next[i] = &node
		}
		s.Size += 1
	}
}

func (s *SkipList) Print() {

	for i:=len(s.head.next) - 1; i>=0; i-- {
		x := s.head
		for x.next[i] != nil  {
			if x.next[i].Tombstone == false {
				fmt.Println(x.next[i].Key, ": ", string(x.next[i].Value))
			}
			x = x.next[i]
		}
	}
}

func (s *SkipList) Return() []*Node {
	x := s.head
	list := make([]*Node, 0)

	for x.next[0] != nil {
		if x.next[0].Tombstone == false {
			list = append(list, x.next[0])
		}
		x = x.next[0]
	}
	return list
}

func max(height int, i int) int {

	if height < i {
		return i
	}
	return height
}

func (s *SkipList) Delete(key *string) {
	update := s.updateList(key)
	x := s.Find(*key, update)
	x.Tombstone = true
}

func roll() int {
	height := 1
	for ; rand.Int31n(2) == 1; height++ {}
	return height
}

