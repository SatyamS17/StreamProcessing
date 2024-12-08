package rainstorm

import (
	"fmt"
	"log"
	"mp4/dht"
	"os"
	"sync"
	"time"
)

const flushPeriod = 100 * time.Millisecond

type BatchLogger struct {
	dhtServer     *dht.Server
	dfsFileName   string
	localFileName string

	mu sync.Mutex

	stop chan struct{}
}

func NewBatchLogger(dhtServer *dht.Server, dfsFileName string) *BatchLogger {
	b := BatchLogger{
		dhtServer:     dhtServer,
		dfsFileName:   dfsFileName,
		localFileName: dfsFileName + ".tmp",
		stop:          make(chan struct{}),
	}

	f, err := os.Create(b.localFileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	f.Close()

	dhtServer.Create(b.localFileName, b.dfsFileName)

	go func() {
		for {
			select {
			case <-b.stop:
				return
			default:
				b.flush()
				time.Sleep(flushPeriod)
			}
		}
	}()

	return &b
}

func (b *BatchLogger) Append(data string) {
	if b == nil {
		fmt.Println("LOGGER IS NULL")
		log.Fatal("Logger is null")
		return
	}

	b.mu.Lock()
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
	b.mu.Unlock()
}

func (b *BatchLogger) Stop() {
	b.flush()
	b.stop <- struct{}{}
}

func (b *BatchLogger) flush() {
	fi, err := os.Stat(b.localFileName)
	if err != nil {
		return
	}

	if fi.Size() > 0 {
		b.mu.Lock()
		err := b.dhtServer.Append(b.localFileName, b.dfsFileName)
		if err != nil {
			fmt.Println(err)
		} else {
			os.Truncate(b.localFileName, 0)
		}
		b.mu.Unlock()
	}
}
