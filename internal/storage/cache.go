package storage

import (
	"sync"

	"github.com/cockroachdb/pebble"
)

const (
	DefaultCacheSize = 128 << 20
)

type Cache struct {
	c    *pebble.Cache
	once sync.Once
}

func NewCache(size int64) *Cache {
	return &Cache{
		c: pebble.NewCache(size),
	}
}

func (cache *Cache) Unref() {
	if cache != nil && cache.c != nil {
		cache.once.Do(func() {
			cache.c.Unref()
		})
	}
}

func (cache *Cache) get() *pebble.Cache {
	if cache == nil {
		return nil
	}
	return cache.c
}
