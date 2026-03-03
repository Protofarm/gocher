package gocher

import (
	"fmt"

	"github.com/alphadose/haxmap"
	"github.com/cespare/xxhash/v2"
)

type ObjectType int

const ShardsCount = 16

// idk if we need more types than string and hash, but we can add more later if we want, everything at the end is just bytes nothing much we care
const (
	StringType ObjectType = iota
	HashType
)

type Object struct {
	Type ObjectType
	Str  string
	Hash *haxmap.Map[string, string]
}

type Cache struct {
	data *haxmap.Map[uint64, Object]
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
			Cache: newCacheWithSize(2048),
		}
	}
	return sc
}

func HashKey(key string) uint32 {
	return uint32(xxhash.Sum64String(key))
}

func hashKey64(key string) uint64 {
	return xxhash.Sum64String(key)
}

func (sc *ShardedCache) getShard(key string) (*Shard, uint64) {
	hash := hashKey64(key)
	return sc.shards[hash&(ShardsCount-1)], hash
}

func newCacheWithSize(size uintptr) *Cache {
	data := haxmap.New[uint64, Object](size)
	data.SetHasher(func(key uint64) uintptr {
		return uintptr(key)
	})
	return &Cache{data: data}
}

func NewCache() *Cache {
	return newCacheWithSize(32768)
}

func (c *Cache) setByHash(keyHash uint64, value string) {
	c.data.Set(keyHash, Object{
		Type: StringType,
		Str:  value,
		Hash: nil,
	})
}

func (c *Cache) Set(key, value string) {
	c.setByHash(hashKey64(key), value)
}

func (c *Cache) getByHash(keyHash uint64) (string, bool, error) {
	obj, exists := c.data.Get(keyHash)
	if !exists {
		return "", false, nil
	}

	if obj.Type != StringType {
		return "", false, fmt.Errorf("WRONGTYPE operation against a key holding wrong kind of value")
	}

	return obj.Str, true, nil
}

func (c *Cache) Get(key string) (string, bool, error) {
	return c.getByHash(hashKey64(key))
}

func (c *Cache) hSetByHash(keyHash uint64, field, value string) error {
	newHashMap := haxmap.New[string, string]()
	newHashMap.Set(field, value)

	obj, loaded := c.data.GetOrSet(keyHash, Object{
		Type: HashType,
		Hash: newHashMap,
	})
	if !loaded {
		return nil
	}

	if obj.Type != HashType {
		return fmt.Errorf("WRONGTYPE operation against a key holding wrong kind of value")
	}

	hashMap := obj.Hash
	if hashMap == nil {
		hashMap = haxmap.New[string, string]()
		obj.Hash = hashMap
		c.data.Set(keyHash, obj)
	}
	hashMap.Set(field, value)
	return nil
}

func (c *Cache) HSet(key, field, value string) error {
	return c.hSetByHash(hashKey64(key), field, value)
}

func (c *Cache) hGetByHash(keyHash uint64, field string) (string, bool, error) {
	obj, exists := c.data.Get(keyHash)
	if !exists {
		return "", false, nil
	}

	if obj.Type != HashType {
		return "", false, fmt.Errorf("WRONGTYPE operation against a key holding wrong kind of value")
	}

	hashMap := obj.Hash
	if hashMap == nil {
		return "", false, nil
	}
	val, ok := hashMap.Get(field)
	return val, ok, nil
}

func (c *Cache) HGet(key, field string) (string, bool, error) {
	return c.hGetByHash(hashKey64(key), field)
}

func (c *Cache) hGetAllByHash(keyHash uint64) (map[string]string, bool, error) {
	obj, exists := c.data.Get(keyHash)
	if !exists {
		return nil, false, nil
	}

	if obj.Type != HashType {
		return nil, false, fmt.Errorf("WRONGTYPE operation against a key holding wrong kind of value")
	}

	all := make(map[string]string)
	hashMap := obj.Hash
	if hashMap == nil {
		return all, true, nil
	}

	hashMap.ForEach(func(field, value string) bool {
		all[field] = value
		return true
	})

	return all, true, nil
}

func (c *Cache) HGetAll(key string) (map[string]string, bool, error) {
	return c.hGetAllByHash(hashKey64(key))
}

func (c *Cache) deleteByHash(keyHash uint64) bool {
	_, exists := c.data.GetAndDel(keyHash)
	return exists
}

func (c *Cache) Delete(key string) bool {
	return c.deleteByHash(hashKey64(key))
}

func (sc *ShardedCache) ShardSet(key, value string) {
	shard, hash := sc.getShard(key)
	shard.Cache.setByHash(hash, value)
}

func (sc *ShardedCache) ShardGet(key string) (string, bool, error) {
	shard, hash := sc.getShard(key)
	return shard.Cache.getByHash(hash)
}

func (sc *ShardedCache) ShardHSet(key, field, value string) error {
	shard, hash := sc.getShard(key)
	return shard.Cache.hSetByHash(hash, field, value)
}

func (sc *ShardedCache) ShardHGet(key, field string) (string, bool, error) {
	shard, hash := sc.getShard(key)
	return shard.Cache.hGetByHash(hash, field)
}

func (sc *ShardedCache) ShardHGetAll(key string) (map[string]string, bool, error) {
	shard, hash := sc.getShard(key)
	return shard.Cache.hGetAllByHash(hash)
}

func (sc *ShardedCache) ShardDelete(key string) bool {
	shard, hash := sc.getShard(key)
	return shard.Cache.deleteByHash(hash)
}
