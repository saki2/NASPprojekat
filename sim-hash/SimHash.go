package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var stopWords = []string{"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"}


type SimHash struct {
	text string
	fingerPrint []int
	mapa map[string]int
}

func (simHash *SimHash) newSimHash() {

	simHash.RemovePunctuation()
	tokens := strings.Split(simHash.text, " ")

	for i, token := range tokens {
		for _, s := range stopWords {
			if s == token {
				tokens[i] = "nil"
			}
		}
	}

	mapa := make(map[string]int)

	for _, token := range tokens {
		if token != "nil" {
			mapa[token] += 1
		}
	}

	simHash.mapa = mapa

	table := make([][]string, len(simHash.mapa))
	for m := range table {
		table[m] = make([]string, 256)
	}

	i := 0
	for key, _ := range simHash.mapa {
		str := ToBinary(GetMD5Hash(key))
		for j:=0; j< len(str); j++ {
			if string(str[j]) == "0" {
				table[i][j] = "-1"
			} else {
				table[i][j] = "1"
			}
		}
		i++
	}
	counter := 0
	vectorV := make([]int, 256)
	for _, value := range simHash.mapa {
		for m := 0; m < len(table[counter]); m++ {
			intValue, _ := strconv.Atoi(table[counter][m])
			vectorV[m] += intValue * value
		}
		counter++
	}

	for e, t := range vectorV {
		if t <= 0 {
			vectorV[e] = 0
		} else {
			vectorV[e] = 1
		}
	}

	simHash.fingerPrint = vectorV

	

}

func hammingDistance(msh1, msh2 SimHash) int {

	n := 0
	for i, el := range msh1.fingerPrint {
		if (el == 0 && msh2.fingerPrint[i] == 1) || (el == 1 && msh2.fingerPrint[i] == 0) {
			n += 1
		}
	}

	return n
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
			res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

func (simHash *SimHash) RemovePunctuation() {

			simHash.text = strings.Replace(simHash.text, ":", "", -1)
			simHash.text = strings.Replace(simHash.text, ",", "", -1)
			simHash.text = strings.Replace(simHash.text, ".", "", -1)

}

func main() {

	file, err := os.Open("sim-hash/files/tekst1.txt")
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Scan()

	file2, err := os.Open("sim-hash/files/tekst2.txt")
	if err != nil {
		panic(err.Error())
	}
	defer file2.Close()
	scanner2 := bufio.NewScanner(file2)
	scanner2.Scan()

	sh := SimHash{scanner.Text(), nil, nil}
	sh.newSimHash()
	sh2 := SimHash{scanner2.Text(), nil, nil}
	sh2.newSimHash()
	fmt.Println(hammingDistance(sh, sh2))

}
