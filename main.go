package gocher

import (
	"fmt"
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

type request struct {
	fn func(c *Cache)
}
type Shard struct {
	Cache *Cache
	reqch chan request
}

type ShardedCache struct {
	shards [ShardsCount]*Shard
}

func NewShardedCache() *ShardedCache {
	sc := &ShardedCache{}
	for i := range sc.shards {
		sc.shards[i] = &Shard{
			Cache: NewCache(),
			reqch: make(chan request, 100), // buffered channel for requests
		}
		go sc.shardWorker(sc.shards[i])
	}
	return sc
}

func (sc *ShardedCache) shardWorker(shard *Shard) {
	for req := range shard.reqch {
		req.fn(shard.Cache)
	}
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
