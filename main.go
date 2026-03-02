package gocher

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type ObjectType int

const ShardsCount = 16

// idk if we need more types than string and hash, but we can add more later if we want, everything at the end is just bytes nothing much we care
const (
	StringType ObjectType = iota
	HashType
)

type Object struct {
	Type  ObjectType
	Value interface{}
}

type Cache struct {
	data map[string]*Object
	mu   sync.RWMutex
}

type Shard struct {
	Cache *Cache
}

type ShardedCache struct {
	shards [ShardsCount]*Shard
}

func NewShardedCache() *ShardedCache {
	sc := &ShardedCache{}
	for i := range sc.shards {
		sc.shards[i] = &Shard{
			Cache: NewCache(),
		}
	}
	return sc
}

func HashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (sc *ShardedCache) getShard(key string) *Shard {
	hash := HashKey(key)
	return sc.shards[hash&(ShardsCount-1)]
}

func NewCache() *Cache {
	return &Cache{data: make(map[string]*Object)}
}

func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = &Object{
		Type:  StringType,
		Value: value,
	}
}

func (c *Cache) Get(key string) (string, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	obj, exists := c.data[key]
	if !exists {
		return "", false, nil
	}

	if obj.Type != StringType {
		return "", false, fmt.Errorf("WRONGTYPE operation against a key holding wrong kind of value")
	}

	return obj.Value.(string), true, nil
}

func (c *Cache) HSet(key, field, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	obj, exists := c.data[key]

	if !exists {
		hash := make(map[string]string)
		hash[field] = value

		c.data[key] = &Object{
			Type:  HashType,
			Value: hash,
		}
		return nil
	}

	if obj.Type != HashType {
		return fmt.Errorf("WRONGTYPE operation against a key holding wrong kind of value")
	}

	hash := obj.Value.(map[string]string)
	hash[field] = value
	return nil
}

func (c *Cache) HGet(key, field string) (string, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	obj, exists := c.data[key]
	if !exists {
		return "", false, nil
	}

	if obj.Type != HashType {
		return "", false, fmt.Errorf("WRONGTYPE operation against a key holding wrong kind of value")
	}

	hash := obj.Value.(map[string]string)
	val, ok := hash[field]
	return val, ok, nil
}

func (c *Cache) HGetAll(key string) (map[string]string, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	obj, exists := c.data[key]
	if !exists {
		return nil, false, nil
	}

	if obj.Type != HashType {
		return nil, false, fmt.Errorf("WRONGTYPE operation against a key holding wrong kind of value")
	}

	return obj.Value.(map[string]string), true, nil
}

func (c *Cache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, exists := c.data[key]
	if exists {
		delete(c.data, key)
	}
	return exists
}

func (sc *ShardedCache) ShardSet(key, value string) {
	shard := sc.getShard(key)
	shard.Cache.Set(key, value)
}

func (sc *ShardedCache) ShardGet(key string) (string, bool, error) {
	shard := sc.getShard(key)
	return shard.Cache.Get(key)
}

func (sc *ShardedCache) ShardHSet(key, field, value string) error {
	shard := sc.getShard(key)
	return shard.Cache.HSet(key, field, value)
}

func (sc *ShardedCache) ShardHGet(key, field string) (string, bool, error) {
	shard := sc.getShard(key)
	return shard.Cache.HGet(key, field)
}

func (sc *ShardedCache) ShardHGetAll(key string) (map[string]string, bool, error) {
	shard := sc.getShard(key)
	return shard.Cache.HGetAll(key)
}

func (sc *ShardedCache) ShardDelete(key string) bool {
	shard := sc.getShard(key)
	return shard.Cache.Delete(key)
}
