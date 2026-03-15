package gocher_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/AnimeKaizoku/cacher"
	gocher "github.com/Protofarm/gocher"
	"github.com/allegro/bigcache/v3"
)

const (
	benchEntryCount = 10000
	benchValueSize  = 128
)

type benchPayload struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var (
	benchOnce   sync.Once
	benchKeys   []string
	benchBytes  [][]byte
	benchStruct []benchPayload

	benchBytesSink  []byte
	benchStructSink benchPayload
)

func loadBenchData() ([]string, [][]byte, []benchPayload) {
	benchOnce.Do(func() {
		benchKeys = make([]string, benchEntryCount)
		benchBytes = make([][]byte, benchEntryCount)
		benchStruct = make([]benchPayload, benchEntryCount)
		for i := 0; i < benchEntryCount; i++ {
			benchKeys[i] = fmt.Sprintf("key-%08d", i)
			b := make([]byte, benchValueSize)
			copy(b, benchKeys[i])
			benchBytes[i] = b
			benchStruct[i] = benchPayload{ID: i, Name: benchKeys[i]}
		}
	})
	return benchKeys, benchBytes, benchStruct
}

func newBenchBigCache(b *testing.B) *bigcache.BigCache {
	b.Helper()
	cache, err := bigcache.NewBigCache(bigcache.Config{
		Shards:             64,
		LifeWindow:         time.Minute,
		MaxEntriesInWindow: benchEntryCount,
		MaxEntrySize:       benchValueSize + 64,
		Verbose:            false,
	})
	if err != nil {
		b.Fatalf("bigcache init failed: %v", err)
	}
	return cache
}

func BenchmarkGocherSetWithoutSerialization(b *testing.B) {
	keys, values, _ := loadBenchData()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cache := gocher.NewCache()
		for n := 0; n < benchEntryCount; n++ {
			cache.Set(keys[n], values[n], 0)
		}
	}
}

func BenchmarkBigCacheSetWithoutSerialization(b *testing.B) {
	keys, values, _ := loadBenchData()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cache := newBenchBigCache(b)
		for n := 0; n < benchEntryCount; n++ {
			if err := cache.Set(keys[n], values[n]); err != nil {
				b.Fatalf("set failed: %v", err)
			}
		}
		cache.Close()
	}
}

func BenchmarkCacherSetWithoutSerialization(b *testing.B) {
	keys, values, _ := loadBenchData()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cache := cacher.NewCacher[string, []byte](nil)
		for n := 0; n < benchEntryCount; n++ {
			cache.Set(keys[n], values[n])
		}
	}
}

func BenchmarkGocherGetWithoutSerialization(b *testing.B) {
	keys, values, _ := loadBenchData()
	cache := gocher.NewCache()
	for n := 0; n < benchEntryCount; n++ {
		cache.Set(keys[n], values[n], 0)
	}
	rng := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Intn(benchEntryCount)
		v, _ := cache.Get(keys[id])
		benchBytesSink = v
	}
}

func BenchmarkBigCacheGetWithoutSerialization(b *testing.B) {
	keys, values, _ := loadBenchData()
	cache := newBenchBigCache(b)
	for n := 0; n < benchEntryCount; n++ {
		if err := cache.Set(keys[n], values[n]); err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
	defer cache.Close()
	rng := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Intn(benchEntryCount)
		v, err := cache.Get(keys[id])
		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
		benchBytesSink = v
	}
}

func BenchmarkCacherGetWithoutSerialization(b *testing.B) {
	keys, values, _ := loadBenchData()
	cache := cacher.NewCacher[string, []byte](nil)
	for n := 0; n < benchEntryCount; n++ {
		cache.Set(keys[n], values[n])
	}
	rng := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Intn(benchEntryCount)
		v, ok := cache.Get(keys[id])
		if !ok {
			b.Fatalf("get failed")
		}
		benchBytesSink = v
	}
}

func BenchmarkGocherSetWithSerialization(b *testing.B) {
	keys, _, values := loadBenchData()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cache := gocher.NewCache()
		for n := 0; n < benchEntryCount; n++ {
			data, err := json.Marshal(values[n])
			if err != nil {
				b.Fatalf("marshal failed: %v", err)
			}
			cache.Set(keys[n], data, 0)
		}
	}
}

func BenchmarkBigCacheSetWithSerialization(b *testing.B) {
	keys, _, values := loadBenchData()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cache := newBenchBigCache(b)
		for n := 0; n < benchEntryCount; n++ {
			data, err := json.Marshal(values[n])
			if err != nil {
				b.Fatalf("marshal failed: %v", err)
			}
			if err := cache.Set(keys[n], data); err != nil {
				b.Fatalf("set failed: %v", err)
			}
		}
		cache.Close()
	}
}

func BenchmarkCacherSetWithSerialization(b *testing.B) {
	keys, _, values := loadBenchData()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cache := cacher.NewCacher[string, []byte](nil)
		for n := 0; n < benchEntryCount; n++ {
			data, err := json.Marshal(values[n])
			if err != nil {
				b.Fatalf("marshal failed: %v", err)
			}
			cache.Set(keys[n], data)
		}
	}
}

func BenchmarkGocherGetWithSerialization(b *testing.B) {
	keys, _, values := loadBenchData()
	cache := gocher.NewCache()
	for n := 0; n < benchEntryCount; n++ {
		data, err := json.Marshal(values[n])
		if err != nil {
			b.Fatalf("marshal failed: %v", err)
		}
		cache.Set(keys[n], data, 0)
	}
	rng := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Intn(benchEntryCount)
		v, _ := cache.Get(keys[id])
		var out benchPayload
		if err := json.Unmarshal(v, &out); err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
		benchStructSink = out
	}
}

func BenchmarkBigCacheGetWithSerialization(b *testing.B) {
	keys, _, values := loadBenchData()
	cache := newBenchBigCache(b)
	for n := 0; n < benchEntryCount; n++ {
		data, err := json.Marshal(values[n])
		if err != nil {
			b.Fatalf("marshal failed: %v", err)
		}
		if err := cache.Set(keys[n], data); err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
	defer cache.Close()
	rng := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Intn(benchEntryCount)
		v, err := cache.Get(keys[id])
		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
		var out benchPayload
		if err := json.Unmarshal(v, &out); err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
		benchStructSink = out
	}
}

func BenchmarkCacherGetWithSerialization(b *testing.B) {
	keys, _, values := loadBenchData()
	cache := cacher.NewCacher[string, []byte](nil)
	for n := 0; n < benchEntryCount; n++ {
		data, err := json.Marshal(values[n])
		if err != nil {
			b.Fatalf("marshal failed: %v", err)
		}
		cache.Set(keys[n], data)
	}
	rng := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Intn(benchEntryCount)
		v, ok := cache.Get(keys[id])
		if !ok {
			b.Fatalf("get failed")
		}
		var out benchPayload
		if err := json.Unmarshal(v, &out); err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
		benchStructSink = out
	}
}
