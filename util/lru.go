package util

import (
	"container/list"
	"fmt"
	"os"
)

const (
	cacheDir = "./cache"
)

type LRUCache struct {
	capacity int
	cache    map[string]*list.Element
	ll       *list.List
}

// NewLRUCache initializes a new LRU cache with a given capacity
func NewLRUCache(capacity int) *LRUCache {
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		os.RemoveAll(cacheDir)
	}

	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		fmt.Println("Unable to create cache directory")
	}

	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		ll:       list.New(),
	}
}

// Put inserts a new key-value pair into the cache
func (lru *LRUCache) Insert(url string, filePath string) {
	if lru.capacity == 0 {
		return
	}

	// If cache is full, evict the least recently used item
	if lru.ll.Len() == lru.capacity {
		lastElem := lru.ll.Back()
		if lastElem != nil {
			lastURL := lastElem.Value.(string)

			os.Remove(urlToCacheFilePath(lastURL))
			lru.ll.Remove(lastElem)
			delete(lru.cache, lastURL)
		}
	}

	CopyFile(filePath, urlToCacheFilePath(url))

	newElem := lru.ll.PushFront(url)
	lru.cache[url] = newElem
}

func (lru *LRUCache) Get(key string) string {
	if elem, ok := lru.cache[key]; ok {
		lru.ll.MoveToFront(elem)
		return urlToCacheFilePath(elem.Value.(string))
	}
	return ""
}

// Exists checks if a key exists in the cache without modifying the order
func (lru *LRUCache) Contains(key string) bool {
	_, exists := lru.cache[key]
	return exists
}

func urlToCacheFilePath(url string) string {
	return fmt.Sprintf("%s/%d", cacheDir, Hash(url))
}
