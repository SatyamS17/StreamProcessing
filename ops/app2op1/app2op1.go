package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

func main() {
	r := csv.NewReader(strings.NewReader(os.Args[2]))
	values, err := r.Read()
	if err != nil {
		return
	}
	pattern := os.Args[3]

	if values[6] == pattern {
		fmt.Println(values[8])
		fmt.Println(1)
	}
}
