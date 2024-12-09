package rainstorm

import (
	"fmt"
	"log"
	"mp4/dht"
	"os"
	"sync"
	"time"
)

// Variables ---------------------------------------------------
type BatchLogger struct {
	dhtServer     *dht.Server
	dfsFileName   string
	localFileName string
	flushPeriod   time.Duration

	mu sync.Mutex

	stop    chan struct{}
	stopped bool
}

// Init a new logger | local - collectes batches    dfs - gets appends in batches
func NewBatchLogger(dhtServer *dht.Server, dfsFileName string, flushPeriod time.Duration) *BatchLogger {
	b := BatchLogger{
		dhtServer:     dhtServer,
		dfsFileName:   dfsFileName,
		localFileName: dfsFileName + ".tmp",
		flushPeriod:   flushPeriod,
		stop:          make(chan struct{}),
	}

	// Create local file
	f, err := os.Create(b.localFileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	f.Close()

	dhtServer.Create(b.localFileName, b.dfsFileName)

	// Flush the local file periodically (write all to dfs file)
	if b.flushPeriod > 0 {
		go func() {
			for {
				select {
				case <-b.stop:
					return
				default:
					b.mu.Lock()
					b.flush()
					b.mu.Unlock()
					time.Sleep(b.flushPeriod)
				}
			}
		}()
	}

	return &b
}

// Appends to the local file for quick updates
func (b *BatchLogger) Append(data string) {
	// Make sure logger is valid
	if b == nil {
		fmt.Println("LOGGER IS NULL")
		os.Exit(1)
	}

	if b.stopped {
		fmt.Println("Appending to stopped logger")
		os.Exit(1)
	}

	// Write to local file
	b.mu.Lock()
	defer b.mu.Unlock()

	f, err := os.OpenFile(b.localFileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte(data)); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

	// Check if we want to flush instantly
	if b.flushPeriod == 0 {
		b.flush()
	}
}

// Closes the logger
func (b *BatchLogger) Stop() {
	b.mu.Lock()
	b.flush()
	b.mu.Unlock()
	if b.flushPeriod > 0 {
		b.stop <- struct{}{}
	}
	b.stopped = true
	os.Remove(b.localFileName)
}

// Copies all local file data to dfs file (clears local file)
func (b *BatchLogger) flush() {
	fi, err := os.Stat(b.localFileName)
	if err != nil {
		return
	}

	// Make sure we have something worth copying
	if fi.Size() > 0 {
		err := b.dhtServer.Append(b.localFileName, b.dfsFileName)
		if err != nil {
			fmt.Println(err)
		} else {
			os.Truncate(b.localFileName, 0)
		}
	}
}
