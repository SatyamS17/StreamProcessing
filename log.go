package main

import (
	"fmt"
	"log"
	"os"
)

func openLogFile(i int) (*os.File, error) {
	logFileName := fmt.Sprintf("machine.%d.log", i+1)

	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
		return nil, err
	}

	log.SetOutput(file)

	return file, nil
}
