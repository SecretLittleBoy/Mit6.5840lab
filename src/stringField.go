package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"unicode"
)

func main() {
	file,err := os.Open("./src/main/pg-being_ernest.txt")
	//Users/yihaolong/Desktop/6.5840/src/main/pg-being_ernest.txt
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	content,err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(string(content), ff)
	for _, w := range words {
		fmt.Println(w)
	}
	os.Exit(0)
}