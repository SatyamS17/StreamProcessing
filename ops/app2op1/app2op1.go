package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	values := strings.Split(os.Args[2], ",")
	pattern := os.Args[3]

	if values[6] == pattern {
		fmt.Println(values[8])
		fmt.Println(1)
	}
}
