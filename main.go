package gocher

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bucketPerShard = 1024
	defaultShards  = 16
)

type entry struct {
	value    []byte
	expireAt int64
	version  uint64
}

type bucket struct {
	ptr        atomic.Pointer[entry]
	accessedAt atomic.Int64
	state      atomic.Uint64 // 0 = uncached (empty), 1 = inbound, 2 = main
}

type shard struct {
	data sync.Map // map[string]*bucket
	keys []string
}

type Cache struct {
	shards    [defaultShards]shard
	currBytes atomic.Uint64
	maxBytes  uint64
}

func NewCache(maxBytes uint64, keys ...string) *Cache {
	c := &Cache{maxBytes: maxBytes}
	for _, key := range keys {
		s := c.getShard(key)
		s.data.LoadOrStore(key, &bucket{})
		s.keys = append(s.keys, key)
	}
	for idx := range c.shards {
		go c.activeTTLHandler(idx)
	}
	return c
}

func (c *Cache) shardIndex(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % defaultShards
}

func (c *Cache) getShard(key string) *shard {
	return &c.shards[c.shardIndex(key)]
}

func (c *Cache) getOrCreateBucket(key string) *bucket {
	s := c.getShard(key)
	if v, ok := s.data.Load(key); ok {
		return v.(*bucket)
	}
	b := &bucket{}
	b.accessedAt.Store(time.Now().Unix())
	b.state.Store(0)
	actual, _ := s.data.LoadOrStore(key, b)
	return actual.(*bucket)
}

func (c *Cache) getBucket(key string) (*bucket, bool) {
	s := c.getShard(key)
	v, ok := s.data.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*bucket), true
}

func validateTTL(expireAt int64) bool {
	return expireAt != 0 && expireAt <= time.Now().Unix()
}

func (c *Cache) GetWithVersion(key string) ([]byte, uint64, bool) {
	b, ok := c.getBucket(key)
	if !ok {
		return nil, 0, false
	}

	e := b.ptr.Load()
	if e == nil {
		return nil, 0, false
	}
	if validateTTL(e.expireAt) {
		if b.ptr.CompareAndSwap(e, nil) {
			c.currBytes.Add(-uint64(len(e.value)))
			b.state.Store(0)
		}
		return nil, 0, false
	}

	b.accessedAt.Store(time.Now().Unix())
	b.state.CompareAndSwap(1, 2)

	return e.value, e.version, true
}

func (c *Cache) Get(key string) ([]byte, bool) {
	v, _, ok := c.GetWithVersion(key)
	return v, ok
}

func (c *Cache) SetWithVersion(key string, val []byte, expected uint64, expires int64) bool {
	sIdx := c.shardIndex(key)
	b := c.getOrCreateBucket(key)

	for {
		old := b.ptr.Load()
		if old == nil {
			if expected != 0 {
				return false
			}

			newEntry := &entry{
				value:    val,
				expireAt: expires,
				version:  1,
			}

			b.state.Store(1)
			b.accessedAt.Store(time.Now().Unix())
			if b.ptr.CompareAndSwap(nil, newEntry) {
				c.currBytes.Add(uint64(len(val)))
				if c.currBytes.Load() > c.maxBytes {
					c.lruEvictionHandler(int(sIdx))
				}
				return true
			}
			continue
		}

		if old.version != expected {
			return false
		}

		newEntry := &entry{
			value:    val,
			expireAt: expires,
			version:  old.version + 1,
		}

		b.accessedAt.Store(time.Now().Unix())
		if b.ptr.CompareAndSwap(old, newEntry) {
			c.currBytes.Add(uint64(len(val) - len(old.value)))
			if c.currBytes.Load() > c.maxBytes {
				c.lruEvictionHandler(int(sIdx))
			}
			return true
		}
	}
}

func (c *Cache) Set(key string, val []byte, expires int64) {
	sIdx := c.shardIndex(key)
	b := c.getOrCreateBucket(key)

	for {
		old := b.ptr.Load()

		var newVersion uint64
		if old == nil {
			newVersion = 1
			b.state.Store(1)
		} else {
			newVersion = old.version + 1
		}

		newEntry := &entry{
			value:    val,
			expireAt: expires,
			version:  newVersion,
		}

		b.accessedAt.Store(time.Now().Unix())
		if b.ptr.CompareAndSwap(old, newEntry) {
			delta := uint64(len(val))
			if old != nil {
				delta -= uint64(len(old.value))
			}
			c.currBytes.Add(delta)
			if c.currBytes.Load() > c.maxBytes {
				c.lruEvictionHandler(int(sIdx))
			}
			return
		}
	}
}

func (c *Cache) Close() error {
	return nil
}

func (c *Cache) activeTTLHandler(sIdx int) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	s := &c.shards[sIdx]
	total := len(s.keys)
	if total == 0 {
		return
	}

	for range ticker.C {
		for {
			sample := 20
			if total < sample {
				sample = total
			}

			expired := 0

			for range sample {
				rIdx := rand.Intn(total)
				key := s.keys[rIdx]

				b, ok := c.getBucket(key)
				if !ok {
					continue
				}
				e := b.ptr.Load()
				if e == nil {
					continue
				}
				if validateTTL(e.expireAt) {
					if b.ptr.CompareAndSwap(e, nil) {
						c.currBytes.Add(-uint64(len(e.value)))
						b.state.Store(0)
					}
					expired++
				}
			}

			if expired <= sample/4 {
				break
			}
		}
	}
}
func (c *Cache) lruEvictionHandler(sIdx int) {
	s := &c.shards[sIdx]
	total := len(s.keys)
	if total == 0 {
		return
	}

	sampleSize := 20
	if total < sampleSize {
		sampleSize = total
	}

	var oldestInb *bucket
	var oldestMain *bucket

	var oldestInbAge int64 = 1<<63 - 1
	var oldestMainAge int64 = 1<<63 - 1
	inboundCount := 0

	for i := 0; i < sampleSize; i++ {
		rIdx := rand.Intn(total)
		key := s.keys[rIdx]

		b, ok := c.getBucket(key)
		if !ok {
			continue
		}

		state := b.state.Load()
		accessedAt := b.accessedAt.Load()

		switch state {
		case 1:
			inboundCount++
			if accessedAt < oldestInbAge {
				oldestInbAge = accessedAt
				oldestInb = b
			}
		case 2:
			if accessedAt < oldestMainAge {
				oldestMainAge = accessedAt
				oldestMain = b
			}
		}
	}

	var targetBucket *bucket
	if inboundCount > sampleSize/4 {
		targetBucket = oldestInb
	} else {
		targetBucket = oldestMain
	}

	if targetBucket == nil {
		if oldestInb != nil {
			targetBucket = oldestInb
		} else {
			targetBucket = oldestMain
		}
	}

	if targetBucket != nil {
		oldEntry := targetBucket.ptr.Load()
		if oldEntry != nil {
			if targetBucket.ptr.CompareAndSwap(oldEntry, nil) {
				targetBucket.state.Store(0)
			}
		}
	}
}
