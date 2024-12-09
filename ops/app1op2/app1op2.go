package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	// key := os.Args[1]
	values := strings.Split(os.Args[2], ",")
	// pattern := os.Args[3]

	fmt.Println(values[2])
	fmt.Println(values[3])
}
