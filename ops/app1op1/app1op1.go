package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	key := os.Args[1]
	value := os.Args[2]
	pattern := os.Args[3]

	if strings.Contains(value, pattern) {
		fmt.Println(key)
		fmt.Println(value)
	}
}
