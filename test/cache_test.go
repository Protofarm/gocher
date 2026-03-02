package test

import (
	"testing"

	"github.com/Protofarm/gocher"
)

func TestCacheSetGetDelete(t *testing.T) {
	cache := gocher.NewCache()

	// String
	cache.Set("name", "vpn")
	val, ok, _ := cache.Get("name")
	if !ok || val != "vpn" {
		t.Errorf("Expected 'vpn', got '%s'", val)
	}
	// Hash
	cache.HSet("user:123", "age", "23")
	cache.HSet("user:123", "email", "vpn@mail.com")

	age, ok, _ := cache.HGet("user:123", "age")
	cache.Set("user:1", "alice")
	if !ok || age != "23" {
		t.Errorf("Expected '23', got '%s'", age)
	}
}
