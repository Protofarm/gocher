package test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Protofarm/gocher"
)

func TestCacheSetGetDelete(t *testing.T) {
	cache := gocher.NewCache()

	cache.Set("name", "vpn")
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

	cache.ShardSet("name", "vpn")

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

	if err := cache.ShardHSet("user:123", "age", "23"); err != nil {
		t.Fatalf("unexpected error on shard hset age: %v", err)
	}
	if err := cache.ShardHSet("user:123", "email", "vpn@mail.com"); err != nil {
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

	cache.ShardSet("k:string", "value")
	if err := cache.ShardHSet("k:string", "field", "value"); err == nil {
		t.Fatalf("expected wrongtype error when using HSET on string key")
	}

	if err := cache.ShardHSet("k:hash", "field", "value"); err != nil {
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
	cache.ShardSet(keyA, "value-a")
	cache.ShardSet(keyB, "value-b")

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
