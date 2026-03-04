package test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Protofarm/gocher"
)

func TestCacheSetGetDelete(t *testing.T) {
	cache := gocher.NewCache()

	cache.Set("name", "vpn", 0)
	val, ok, err := cache.Get("name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || val != "vpn" {
		t.Fatalf("expected key name=vpn, got ok=%v val=%q", ok, val)
	}

	deleted := cache.Delete("name")
	if !deleted {
		t.Fatalf("expected delete(name)=true")
	}
	deleted = cache.Delete("name")
	if deleted {
		t.Fatalf("expected delete(name)=false on missing key")
	}
}

func TestShardedCacheSetGetDelete(t *testing.T) {
	cache := gocher.NewShardedCache()

	cache.ShardSet("name", "vpn", 0)

	val, ok, err := cache.ShardGet("name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || val != "vpn" {
		t.Fatalf("expected key name=vpn, got ok=%v val=%q", ok, val)
	}

	deleted := cache.ShardDelete("name")
	if !deleted {
		t.Fatalf("expected shard delete(name)=true")
	}
	deleted = cache.ShardDelete("name")
	if deleted {
		t.Fatalf("expected shard delete(name)=false on missing key")
	}
}

func TestShardedCacheHashOps(t *testing.T) {
	cache := gocher.NewShardedCache()

	if err := cache.ShardHSet("user:123", "age", "23", 0); err != nil {
		t.Fatalf("unexpected error on shard hset age: %v", err)
	}
	if err := cache.ShardHSet("user:123", "email", "vpn@mail.com", 0); err != nil {
		t.Fatalf("unexpected error on shard hset email: %v", err)
	}

	age, ok, err := cache.ShardHGet("user:123", "age")
	if err != nil {
		t.Fatalf("unexpected error on shard hget age: %v", err)
	}
	if !ok || age != "23" {
		t.Fatalf("expected user:123 age=23, got ok=%v val=%q", ok, age)
	}

	all, ok, err := cache.ShardHGetAll("user:123")
	if err != nil {
		t.Fatalf("unexpected error on shard hgetall: %v", err)
	}
	if !ok {
		t.Fatalf("expected user:123 to exist")
	}
	if len(all) != 2 || all["age"] != "23" || all["email"] != "vpn@mail.com" {
		t.Fatalf("unexpected hash fields: %+v", all)
	}
}

func TestShardedCacheWrongTypeErrors(t *testing.T) {
	cache := gocher.NewShardedCache()

	cache.ShardSet("k:string", "value", 0)
	if err := cache.ShardHSet("k:string", "field", "value", 0); err == nil {
		t.Fatalf("expected wrongtype error when using HSET on string key")
	}

	if err := cache.ShardHSet("k:hash", "field", "value", 0); err != nil {
		t.Fatalf("unexpected error on shard hset: %v", err)
	}
	_, _, err := cache.ShardGet("k:hash")
	if err == nil {
		t.Fatalf("expected wrongtype error when using GET on hash key")
	}
	if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE error, got %v", err)
	}
}

func TestShardedCacheDifferentShardsIsolation(t *testing.T) {
	cache := gocher.NewShardedCache()

	keyA, keyB, shardA, shardB := keysOnDifferentShards()
	cache.ShardSet(keyA, "value-a", 0)
	cache.ShardSet(keyB, "value-b", 0)

	valA, ok, err := cache.ShardGet(keyA)
	if err != nil || !ok || valA != "value-a" {
		t.Fatalf("expected %s=value-a, got ok=%v val=%q err=%v", keyA, ok, valA, err)
	}
	valB, ok, err := cache.ShardGet(keyB)
	if err != nil || !ok || valB != "value-b" {
		t.Fatalf("expected %s=value-b, got ok=%v val=%q err=%v", keyB, ok, valB, err)
	}

	if cache.ShardDelete(keyA) != true {
		t.Fatalf("expected delete(%s)=true", keyA)
	}

	valB, ok, err = cache.ShardGet(keyB)
	if err != nil || !ok || valB != "value-b" {
		t.Fatalf("expected %s to remain intact on shard %d after deleting key on shard %d", keyB, shardB, shardA)
	}
}

func keysOnDifferentShards() (string, string, uint32, uint32) {
	keyA := "shard-key-0"
	shardA := gocher.HashKey(keyA) & (gocher.ShardsCount - 1)

	for i := 1; i < 1000; i++ {
		keyB := fmt.Sprintf("shard-key-%d", i)
		shardB := gocher.HashKey(keyB) & (gocher.ShardsCount - 1)
		if shardA != shardB {
			return keyA, keyB, shardA, shardB
		}
	}

	// Fallback should never happen with ShardsCount=16 and this search range.
	return keyA, "shard-key-fallback", shardA, shardA
}

func TestCacheExpiration(t *testing.T) {
	cache := gocher.NewCache()

	// Test with future expiration
	futureTime := time.Now().Unix() + 3600
	cache.Set("key1", "value1", futureTime)
	val, ok, err := cache.Get("key1")
	if err != nil {
		t.Fatalf("unexpected error for non-expired key: %v", err)
	}
	if !ok || val != "value1" {
		t.Fatalf("expected key1=value1, got ok=%v val=%q", ok, val)
	}

	// Test with past expiration
	pastTime := time.Now().Unix() - 3600
	cache.Set("key2", "value2", pastTime)
	_, _, err = cache.Get("key2")
	if err == nil {
		t.Fatalf("expected expired key error, got nil")
	}
	if !strings.Contains(err.Error(), "EXPIRED") {
		t.Fatalf("expected EXPIRED error, got %v", err)
	}

	// Test with no expiration
	cache.Set("key3", "value3", 0)
	val, ok, err = cache.Get("key3")
	if err != nil {
		t.Fatalf("unexpected error for key with no expiration: %v", err)
	}
	if !ok || val != "value3" {
		t.Fatalf("expected key3=value3, got ok=%v val=%q", ok, val)
	}
}

func TestShardedCacheExpiration(t *testing.T) {
	cache := gocher.NewShardedCache()

	futureTime := time.Now().Unix() + 3600
	cache.ShardSet("key1", "value1", futureTime)
	val, ok, err := cache.ShardGet("key1")
	if err != nil {
		t.Fatalf("unexpected error for non-expired key: %v", err)
	}
	if !ok || val != "value1" {
		t.Fatalf("expected key1=value1, got ok=%v val=%q", ok, val)
	}

	pastTime := time.Now().Unix() - 3600
	cache.ShardSet("key2", "value2", pastTime)
	_, _, err = cache.ShardGet("key2")
	if err == nil {
		t.Fatalf("expected expired key error, got nil")
	}
	if !strings.Contains(err.Error(), "EXPIRED") {
		t.Fatalf("expected EXPIRED error, got %v", err)
	}

	cache.ShardSet("key3", "value3", 0)
	val, ok, err = cache.ShardGet("key3")
	if err != nil {
		t.Fatalf("unexpected error for key with no expiration: %v", err)
	}
	if !ok || val != "value3" {
		t.Fatalf("expected key3=value3, got ok=%v val=%q", ok, val)
	}
}

func TestHashExpiration(t *testing.T) {
	cache := gocher.NewShardedCache()

	futureTime := time.Now().Unix() + 3600
	if err := cache.ShardHSet("user:1", "name", "alice", futureTime); err != nil {
		t.Fatalf("unexpected error on shard hset: %v", err)
	}
	name, ok, err := cache.ShardHGet("user:1", "name")
	if err != nil {
		t.Fatalf("unexpected error for non-expired hash: %v", err)
	}
	if !ok || name != "alice" {
		t.Fatalf("expected user:1 name=alice, got ok=%v val=%q", ok, name)
	}

	pastTime := time.Now().Unix() - 3600
	if err := cache.ShardHSet("user:2", "name", "bob", pastTime); err != nil {
		t.Fatalf("unexpected error on shard hset: %v", err)
	}
	_, _, err = cache.ShardHGet("user:2", "name")
	if err == nil {
		t.Fatalf("expected expired key error, got nil")
	}
	if !strings.Contains(err.Error(), "EXPIRED") {
		t.Fatalf("expected EXPIRED error, got %v", err)
	}
}
