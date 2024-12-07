package rainstorm

import (
	"fmt"
	"log"
	"mp4/dht"
	"os"
	"time"
)

const flushPeriod = 1 * time.Second

type BatchLogger struct {
	dhtServer     *dht.Server
	dfsFileName   string
	localFileName string

	stop chan struct{}
}

func NewBatchLogger(dhtServer *dht.Server, dfsFileName string) *BatchLogger {
	b := BatchLogger{dhtServer, dfsFileName, dfsFileName + ".tmp", make(chan struct{})}

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
	f, err := os.OpenFile("access.log", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte(data)); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

func (b *BatchLogger) Stop() {
	b.stop <- struct{}{}
}

func (b *BatchLogger) flush() {
	fi, err := os.Stat(b.localFileName)
	if err != nil {
		return
	}

	if fi.Size() > 0 {
		b.dhtServer.Append(b.localFileName, b.dfsFileName)
		os.Truncate(b.localFileName, 0)
	}
}
