package rainstorm

import (
	"fmt"
	"log"
	"mp4/dht"
	"os"
	"sync"
	"time"
)

type BatchLogger struct {
	dhtServer     *dht.Server
	dfsFileName   string
	localFileName string
	flushPeriod   time.Duration

	mu sync.Mutex

	stop    chan struct{}
	stopped bool
}

func NewBatchLogger(dhtServer *dht.Server, dfsFileName string, flushPeriod time.Duration) *BatchLogger {
	b := BatchLogger{
		dhtServer:     dhtServer,
		dfsFileName:   dfsFileName,
		localFileName: dfsFileName + ".tmp",
		flushPeriod:   flushPeriod,
		stop:          make(chan struct{}),
	}

	f, err := os.Create(b.localFileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	f.Close()

	dhtServer.Create(b.localFileName, b.dfsFileName)

	if b.flushPeriod > 0 {
		go func() {
			for {
				select {
				case <-b.stop:
					return
				default:
					b.flush()
					time.Sleep(b.flushPeriod)
				}
			}
		}()
	}

	return &b
}

func (b *BatchLogger) Append(data string) {
	if b == nil {
		fmt.Println("LOGGER IS NULL")
		log.Fatal("Logger is null")
		return
	}

	if b.stopped {
		log.Fatal("Appending to stopped logger")
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

	if b.flushPeriod == 0 {
		b.flush()
	}
}

func (b *BatchLogger) Stop() {
	b.flush()
	b.stop <- struct{}{}
	b.stopped = true
	os.Remove(b.localFileName)
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
