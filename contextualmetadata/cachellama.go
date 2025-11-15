package contextualmetadata

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

type CacheEntry struct {
	Result    map[string]any
	Timestamp time.Time
}

type MetadataCache struct {
	mu      sync.RWMutex
	entries map[string]*CacheEntry
	ttl     time.Duration
}

func NewMetadataCache(ttl time.Duration) *MetadataCache {
	cache := &MetadataCache{
		entries: make(map[string]*CacheEntry),
		ttl:     ttl,
	}

	// Start cleanup goroutine
	go cache.cleanup()

	return cache
}

func (c *MetadataCache) Get(db, table string) (map[string]any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := c.makeKey(db, table)
	entry, exists := c.entries[key]

	if !exists {
		return nil, false
	}

	// Check if expired
	if time.Since(entry.Timestamp) > c.ttl {
		return nil, false
	}

	return entry.Result, true
}

func (c *MetadataCache) Set(db, table string, result map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.makeKey(db, table)
	c.entries[key] = &CacheEntry{
		Result:    result,
		Timestamp: time.Now(),
	}
}

func (c *MetadataCache) makeKey(db, table string) string {
	h := sha256.Sum256([]byte(db + "/" + table))
	return hex.EncodeToString(h[:])
}

func (c *MetadataCache) cleanup() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		for key, entry := range c.entries {
			if time.Since(entry.Timestamp) > c.ttl {
				delete(c.entries, key)
			}
		}
		c.mu.Unlock()
	}
}
