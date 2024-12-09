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

	fmt.Println(values[2])
	fmt.Println(values[3])
}
