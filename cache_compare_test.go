package gocher_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/AnimeKaizoku/cacher"
	gocher "github.com/Protofarm/gocher"
	"github.com/allegro/bigcache/v3"
)

type comparePayload struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func newBigCache(t *testing.T) *bigcache.BigCache {
	t.Helper()
	cache, err := bigcache.NewBigCache(bigcache.Config{
		Shards:             64,
		LifeWindow:         time.Minute,
		MaxEntriesInWindow: 1024,
		MaxEntrySize:       256,
		Verbose:            false,
	})
	if err != nil {
		t.Fatalf("bigcache init failed: %v", err)
	}
	return cache
}

func TestGocherSetGetWithoutSerialization(t *testing.T) {
	cache := gocher.NewCache()
	key := "plain:key"
	want := []byte("plain-value")

	cache.Set(key, want, 0)
	got, ok := cache.Get(key)
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected value: got=%q want=%q", string(got), string(want))
	}
}

func TestGocherSetGetWithSerialization(t *testing.T) {
	cache := gocher.NewCache()
	key := "json:key"
	want := comparePayload{ID: 7, Name: "gocher"}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	cache.Set(key, encoded, 0)
	gotRaw, ok := cache.Get(key)
	if !ok {
		t.Fatalf("expected key to exist")
	}

	var got comparePayload
	if err := json.Unmarshal(gotRaw, &got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got != want {
		t.Fatalf("unexpected value: got=%+v want=%+v", got, want)
	}
}

func TestBigCacheSetGetWithoutSerialization(t *testing.T) {
	cache := newBigCache(t)
	defer cache.Close()

	key := "plain:key"
	want := []byte("plain-value")

	if err := cache.Set(key, want); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	got, err := cache.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected value: got=%q want=%q", string(got), string(want))
	}
}

func TestBigCacheSetGetWithSerialization(t *testing.T) {
	cache := newBigCache(t)
	defer cache.Close()

	key := "json:key"
	want := comparePayload{ID: 8, Name: "bigcache"}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	if err := cache.Set(key, encoded); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	gotRaw, err := cache.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	var got comparePayload
	if err := json.Unmarshal(gotRaw, &got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got != want {
		t.Fatalf("unexpected value: got=%+v want=%+v", got, want)
	}
}

func TestCacherSetGetWithoutSerialization(t *testing.T) {
	cache := cacher.NewCacher[string, []byte](nil)
	key := "plain:key"
	want := []byte("plain-value")

	cache.Set(key, want)
	got, ok := cache.Get(key)
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected value: got=%q want=%q", string(got), string(want))
	}
}

func TestCacherSetGetWithSerialization(t *testing.T) {
	cache := cacher.NewCacher[string, []byte](nil)
	key := "json:key"
	want := comparePayload{ID: 9, Name: "cacher"}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	cache.Set(key, encoded)
	gotRaw, ok := cache.Get(key)
	if !ok {
		t.Fatalf("expected key to exist")
	}

	var got comparePayload
	if err := json.Unmarshal(gotRaw, &got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got != want {
		t.Fatalf("unexpected value: got=%+v want=%+v", got, want)
	}
}
