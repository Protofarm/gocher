package gocher

import (
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	segmentCount  = 16
	bucketsPerSeg = 56
	stashPerSeg   = 4
	slotCapacity  = 4
)

type entry struct {
	key      string
	value    []byte
	expireAt int64
}

type bucket struct {
	items []entry
}

type segment struct {
	buckets [bucketsPerSeg]*bucket
	stash   [stashPerSeg]*bucket
	lock    sync.RWMutex
}

type DragonCache struct {
	segments []*segment
}

func NewDragonCache() *DragonCache {
	dc := &DragonCache{
		segments: make([]*segment, segmentCount),
	}
	for i := 0; i < segmentCount; i++ {
		seg := &segment{}
		for j := 0; j < bucketsPerSeg; j++ {
			seg.buckets[j] = &bucket{items: make([]entry, 0, slotCapacity)}
		}
		for j := 0; j < stashPerSeg; j++ {
			seg.stash[j] = &bucket{items: make([]entry, 0, slotCapacity)}
		}
		dc.segments[i] = seg
	}
	return dc
}

const ShardsCount = uint32(segmentCount)

func HashKey(key string) uint32 {
	return uint32(xxhash.Sum64String(key))
}

func NewShardedCache() *DragonCache {
	return NewDragonCache()
}

func hashTwo(key string) (uint64, uint64) {
	h := xxhash.Sum64String(key)
	return h % bucketsPerSeg, (h >> 32) % bucketsPerSeg
}

func (dc *DragonCache) getSegment(key string) *segment {
	h := xxhash.Sum64String(key)
	idx := int(h % uint64(len(dc.segments)))
	return dc.segments[idx]
}

func (dc *DragonCache) Set(key string, val []byte, expires int64) {
	seg := dc.getSegment(key)
	idx1, idx2 := hashTwo(key)

	seg.lock.Lock()
	defer seg.lock.Unlock()

	if insertOrUpdate(seg.buckets[idx1], key, val, expires) ||
		insertOrUpdate(seg.buckets[idx2], key, val, expires) {
		return
	}
	for _, s := range seg.stash {
		if insertOrUpdate(s, key, val, expires) {
			return
		}
	}
}

func insertOrUpdate(b *bucket, key string, val []byte, expires int64) bool {
	for i, e := range b.items {
		if e.key == key {
			b.items[i] = entry{key, val, expires}
			return true
		}
	}
	if len(b.items) < slotCapacity {
		b.items = append(b.items, entry{key, val, expires})
		return true
	}
	return false
}

func (dc *DragonCache) Get(key string) ([]byte, bool, error) {
	seg := dc.getSegment(key)
	idx1, idx2 := hashTwo(key)

	seg.lock.RLock()
	defer seg.lock.RUnlock()

	search := func(b *bucket) ([]byte, bool) {
		for _, e := range b.items {
			if e.key == key {
				if e.expireAt != 0 && e.expireAt <= time.Now().Unix() {
					return nil, false
				}
				return e.value, true
			}
		}
		return nil, false
	}

	if val, ok := search(seg.buckets[idx1]); ok {
		return val, true, nil
	}
	if val, ok := search(seg.buckets[idx2]); ok {
		return val, true, nil
	}
	for _, s := range seg.stash {
		if val, ok := search(s); ok {
			return val, true, nil
		}
	}
	return nil, false, nil
}

func (dc *DragonCache) Delete(key string) bool {
	seg := dc.getSegment(key)
	idx1, idx2 := hashTwo(key)

	seg.lock.Lock()
	defer seg.lock.Unlock()
	removed := false
	remove := func(b *bucket) {
		for i, e := range b.items {
			if e.key == key {
				b.items[i] = b.items[len(b.items)-1]
				b.items = b.items[:len(b.items)-1]
				removed = true
				return
			}
		}
	}

	remove(seg.buckets[idx1])
	remove(seg.buckets[idx2])
	for _, s := range seg.stash {
		remove(s)
	}
	return removed
}

func (dc *DragonCache) Close() error {
	return nil
}
