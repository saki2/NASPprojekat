package main

import (
	"crypto/sha1"
	"encoding/hex"
	"log"
	"os"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string{
	return mr.root.String()
}

type Node struct {
	data [20]byte
	left *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

// hash node by hashes of children
func  hash(n1, n2 Node) [20]byte {
	var l, r [20]byte
	l = n1.data
	r = n2.data
	return Hash(append(l[:], r[:]...))
}

func (mr *MerkleRoot) serialize(filename string) {

	File, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer File.Close()
	PreorderRecursive(mr.root, File)

}

func PreorderRecursive(root *Node, file *os.File) {
	if root != nil {
		file.WriteString(root.String() + "\n")
		PreorderRecursive(root.left, file)
		PreorderRecursive(root.right, file)
	}
}

func buildTree(parts []Node) *Node {
	var nodes []Node
	for i := 0; i < len(parts); i += 2 {
		if i + 1 < len(parts) {
			nodes = append(nodes, Node{left: &parts[i], right: &parts[i+1], data: hash(parts[i], parts[i+1])})
		} else {
			nodes = append(nodes, Node{left: &parts[i], right: &Node{}, data: parts[i].data})
		}
	}
	if len(nodes) == 1 {
		return &nodes[0]
	} else if len(nodes) > 1 {
		return buildTree(nodes)
	} else {
		panic("huh?!")
	}
}

func buildTreeLeaf(parts [][20]byte) *Node {
	var nodes []Node
	for i := 0; i < len(parts); i ++ {
		nodes = append(nodes, Node{left: &Node{}, right: &Node{}, data: parts[i]})
	}
	return buildTree(nodes)
}

func main(){
	arr := []string{"This", "is", "the", "first", "tutorial",
		"of", "Merkle", "tree"}
	hashVal := make([][20]byte, 8)

	for i:=0; i < len(arr); i++ {
		hashVal[i] = Hash([]byte(arr[i]))
	}

	Root := buildTreeLeaf(hashVal)
	merkle := MerkleRoot{ root: Root}

	merkle.serialize("vezbe7/Metadata.txt")


}