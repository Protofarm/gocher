package gocher

import (
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	shardCount     = 64
	bucketPerShard = 1024
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
	buckets [bucketPerShard]bucket
}

type Cache struct {
	shards [shardCount]shard
}

func NewCache() *Cache {
	return &Cache{}
}

func hashKey(key string) (uint64, uint64) {
	h := xxhash.Sum64String(key)
	return h % shardCount, h % bucketPerShard
}

func (c *Cache) GetWithVersion(key string) ([]byte, uint64, bool) {
	sIdx, bIdx := hashKey(key)
	b := &c.shards[sIdx].buckets[bIdx]

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
	sIdx, bIdx := hashKey(key)
	b := &c.shards[sIdx].buckets[bIdx]

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

func (c *Cache) DeleteWithVersion(key string, expected uint64) bool {
	sIdx, bIdx := hashKey(key)
	b := &c.shards[sIdx].buckets[bIdx]

	for {
		old := b.ptr.Load()
		if old == nil {
			return false
		}
		if old.version != expected {
			return false
		}
		if b.ptr.CompareAndSwap(old, nil) {
			return true
		}
	}
}

func (c *Cache) Set(key string, val []byte, expires int64) {
	sIdx, bIdx := hashKey(key)
	b := &c.shards[sIdx].buckets[bIdx]

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

func (c *Cache) Delete(key string) bool {
	sIdx, bIdx := hashKey(key)
	b := &c.shards[sIdx].buckets[bIdx]

	for {
		old := b.ptr.Load()
		if old == nil {
			return false
		}
		if b.ptr.CompareAndSwap(old, nil) {
			return true
		}
	}
}

func (c *Cache) Close() error {
	return nil
}
