package gocher

import (
	"hash/fnv"
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
	ptr atomic.Pointer[entry]
}

type shard struct {
	data sync.Map // map[string]*bucket
}

type Cache struct {
	shards [defaultShards]shard
}

func NewCache(keys ...string) *Cache {
	c := &Cache{}
	for _, key := range keys {
		s := c.getShard(key)
		s.data.LoadOrStore(key, &bucket{})
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

func (c *Cache) GetWithVersion(key string) ([]byte, uint64, bool) {
	b, ok := c.getBucket(key)
	if !ok {
		return nil, 0, false
	}

	e := b.ptr.Load()
	if e == nil {
		return nil, 0, false
	}
	if e.expireAt != 0 && e.expireAt <= time.Now().Unix() {
		return nil, 0, false
	}

	return e.value, e.version, true
}

func (c *Cache) Get(key string) ([]byte, bool) {
	v, _, ok := c.GetWithVersion(key)
	return v, ok
}

func (c *Cache) SetWithVersion(key string, val []byte, expected uint64, expires int64) bool {
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
			if b.ptr.CompareAndSwap(nil, newEntry) {
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
		if b.ptr.CompareAndSwap(old, newEntry) {
			return true
		}
	}
}

func (c *Cache) Set(key string, val []byte, expires int64) {
	b := c.getOrCreateBucket(key)

	for {
		old := b.ptr.Load()

		var newVersion uint64
		if old == nil {
			newVersion = 1
		} else {
			newVersion = old.version + 1
		}

		newEntry := &entry{
			value:    val,
			expireAt: expires,
			version:  newVersion,
		}
		if b.ptr.CompareAndSwap(old, newEntry) {
			return
		}
	}
}

func (c *Cache) Close() error {
	return nil
}
