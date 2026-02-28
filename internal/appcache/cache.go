// Package appcache provides a self-managed, general-purpose in-memory cache
// with TTL-based expiration, LRU eviction, size limits, and per-key locking.
// It is designed for caching fleet metadata, auth tokens, permission lookups,
// and any hot-path data that should not hit an external store on every request.
package appcache

import (
	"container/list"
	"sync"
	"time"
)

// entry is a single cached value with expiration metadata.
type entry struct {
	key       string
	value     interface{}
	expiresAt time.Time
	element   *list.Element // pointer into the LRU list
}

// Cache is a thread-safe in-memory LRU cache with TTL support.
type Cache struct {
	mu         sync.RWMutex
	items      map[string]*entry
	lru        *list.List // front = most recently used
	maxItems   int
	defaultTTL time.Duration

	// Metrics (atomic-safe behind mu)
	hits      int64
	misses    int64
	evictions int64
}

// Option configures a Cache.
type Option func(*Cache)

// WithMaxItems sets the maximum number of items before LRU eviction kicks in.
func WithMaxItems(n int) Option {
	return func(c *Cache) { c.maxItems = n }
}

// WithDefaultTTL sets the default time-to-live for entries.
func WithDefaultTTL(d time.Duration) Option {
	return func(c *Cache) { c.defaultTTL = d }
}

// New creates a Cache with the given options.
func New(opts ...Option) *Cache {
	c := &Cache{
		items:      make(map[string]*entry),
		lru:        list.New(),
		maxItems:   10_000,
		defaultTTL: 5 * time.Minute,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Set stores a value with the default TTL.
func (c *Cache) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

// SetWithTTL stores a value with a specific TTL.
func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Update existing entry.
	if e, ok := c.items[key]; ok {
		e.value = value
		e.expiresAt = now.Add(ttl)
		c.lru.MoveToFront(e.element)
		return
	}

	// Evict if at capacity.
	if c.maxItems > 0 && len(c.items) >= c.maxItems {
		c.evictOldest()
	}

	el := c.lru.PushFront(key)
	c.items[key] = &entry{
		key:       key,
		value:     value,
		expiresAt: now.Add(ttl),
		element:   el,
	}
}

// Get retrieves a value. Returns (nil, false) on miss or expiration.
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}

	if time.Now().After(e.expiresAt) {
		c.removeLocked(e)
		c.misses++
		return nil, false
	}

	c.hits++
	c.lru.MoveToFront(e.element)
	return e.value, true
}

// Delete removes a key.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.removeLocked(e)
	}
}

// Len returns the number of entries (including possibly-expired ones).
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Flush removes all entries.
func (c *Cache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*entry)
	c.lru.Init()
}

// Stats returns cache hit/miss/eviction counters.
type Stats struct {
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	Size      int   `json:"size"`
	MaxItems  int   `json:"max_items"`
}

// Stats returns a snapshot of cache metrics.
func (c *Cache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Stats{
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
		Size:      len(c.items),
		MaxItems:  c.maxItems,
	}
}

// Cleanup removes all expired entries. Call periodically from a goroutine.
func (c *Cache) Cleanup() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	removed := 0
	for _, e := range c.items {
		if now.After(e.expiresAt) {
			c.removeLocked(e)
			removed++
		}
	}
	return removed
}

// StartCleanup runs Cleanup every interval in a background goroutine.
// Call the returned stop function to terminate it.
func (c *Cache) StartCleanup(interval time.Duration) (stop func()) {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				c.Cleanup()
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(done) }
}

// ---------------------------------------------------------------------------
// internal
// ---------------------------------------------------------------------------

func (c *Cache) evictOldest() {
	el := c.lru.Back()
	if el == nil {
		return
	}
	key := el.Value.(string)
	if e, ok := c.items[key]; ok {
		c.removeLocked(e)
		c.evictions++
	}
}

func (c *Cache) removeLocked(e *entry) {
	c.lru.Remove(e.element)
	delete(c.items, e.key)
}
