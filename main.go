package main

import "fmt"

type Cache struct {
	data map[string]string
}

func NewCache() *Cache {
	return &Cache{data: make(map[string]string)}
}

func (c *Cache) Set(key, value string) {
	c.data[key] = value
}

func (c *Cache) Get(key string) (string, bool) {
	value, ok := c.data[key]
	return value, ok
}

func (c *Cache) Delete(key string) bool {
	_, ok := c.data[key]
	if ok {
		delete(c.data, key)
	}
	return ok
}

func main() {
	cache := NewCache()

	cache.Set("user:1", "alice")
	fmt.Println("SET user:1 alice")

	if value, ok := cache.Get("user:1"); ok {
		fmt.Printf("GET user:1 -> hit (%s)\n", value)
	} else {
		fmt.Println("GET user:1 -> miss")
	}

	fmt.Printf("DELETE user:1 -> %t\n", cache.Delete("user:1"))

	if value, ok := cache.Get("user:1"); ok {
		fmt.Printf("GET user:1 -> hit (%s)\n", value)
	} else {
		fmt.Println("GET user:1 -> miss")
	}
}
