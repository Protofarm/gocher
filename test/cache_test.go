package test

import (
	"testing"

	"github.com/Protofarm/gocher"
)

func TestCacheSetGetDelete(t *testing.T) {
	cache := gocher.NewCache()

	cache.Set("user:1", "alice")

	value, ok := cache.Get("user:1")
	if !ok {
		t.Fatalf("expected key user:1 to exist")
	}
	if value != "alice" {
		t.Fatalf("expected value alice, got %q", value)
	}

	if !cache.Delete("user:1") {
		t.Fatalf("expected delete to return true for existing key")
	}

	_, ok = cache.Get("user:1")
	if ok {
		t.Fatalf("expected key user:1 to be deleted")
	}
}
