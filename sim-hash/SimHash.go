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

