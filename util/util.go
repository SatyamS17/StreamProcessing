package util

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
)

var (
	downloadCache *LRUCache = NewLRUCache(0)
)

func AddressToID(address string) int {
	re := regexp.MustCompile(`39(\d{2})`)
	match := re.FindStringSubmatch(address)
	num, _ := strconv.Atoi(match[1])

	return num - 1
}

func IDToAddress(id int) string {
	return fmt.Sprintf("fa24-cs425-39%02d.cs.illinois.edu", id+1)
}

func Hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

// Send a UDP packet to a specified target address
func SendUDPPacket(p interface{}, address string, port string) error {
	return sendPacket(p, address, port, "udp")
}

// Send a TCP packet to a specified target address
func SendTCPPacket(p interface{}, address string, port string) error {
	return sendPacket(p, address, port, "tcp")
}

// Copy locally
func CopyFile(from string, to string) error {
	fromFile, err := os.Open(from)
	if err != nil {
		return err
	}
	defer fromFile.Close()

	toFile, err := os.Create(to)
	if err != nil {
		return err
	}
	defer toFile.Close()

	if _, err = io.Copy(toFile, fromFile); err != nil {
		return err
	}

	if err := toFile.Sync(); err != nil {
		return err
	}

	return err
}

func DownloadFile(url string, to string) error {
	if downloadCache.Contains(url) {
		if err := CopyFile(downloadCache.Get(url), to); err != nil {
			log.Println("Unable to use cached download.", err)
		} else {
			fmt.Printf("Downloaded %s from cache of %s\n", to, url)
			return nil
		}
	}

	// Send a GET request to the URL
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error downloading file: %w", err)
	}
	defer resp.Body.Close()

	// Check if the server response is OK (200)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download file: %s", resp.Status)
	}

	// Create a local file to store the downloaded content
	out, err := os.Create(to)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer out.Close()

	// Copy the content from the response body to the local file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("error saving file: %w", err)
	}

	downloadCache.Insert(url, to)

	return nil
}

func sendPacket(p interface{}, address string, port string, network string) error {
	// Encode packet to bytes array
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(p); err != nil {
		return err
	}

	conn, err := net.Dial(network, address+":"+port)

	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err = conn.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func Cat(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}

	defer file.Close()

	_, err = io.Copy(os.Stdout, file)
	if err != nil {
		fmt.Printf("Error outputting file: %v\n", err)
	}
}

func Ls() {
	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	for _, file := range files {
		fmt.Println(file.Name())
	}
}

func HashFile(filePath string, hashFunc func() hash.Hash) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := hashFunc()
	buf := make([]byte, 32*1024)

	if _, err := io.CopyBuffer(hasher, file, buf); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func HashLine(line string) int {
	hash := sha256.Sum256([]byte(line))
	return 1 + int(hash[0])%10
}

func SwapFiles(file1 string, file2 string) {
	// Temporary file name for swapping
	tempFile := file1 + ".temp"

	// Step 1: Rename file1 to a temporary name
	if err := os.Rename(file1, tempFile); err != nil {
		log.Printf("failed to rename %s to %s: %v", file1, tempFile, err)
		return
	}

	// Step 2: Rename file2 to file1's name
	if err := os.Rename(file2, file1); err != nil {
		// If renaming file2 fails, undo the previous rename and return the error
		os.Rename(tempFile, file1) // Undo renaming of file1
		log.Printf("failed to rename %s to %s: %v", file2, file1, err)
		return
	}

	// Step 3: Rename temporary file to file2's name
	if err := os.Rename(tempFile, file2); err != nil {
		// Undo renaming of file2 and file1
		os.Rename(file1, file2) // Undo renaming of file2
		log.Printf("failed to rename %s to %s: %v", tempFile, file2, err)
		return
	}
}

func WriteJSONFile(filePath string, array []string) error {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(array)
	if err != nil {
		return err
	}

	return nil
}
