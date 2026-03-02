package gocher

import (
	"fmt"
)

type ObjectType int

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
}

func NewCache() *Cache {
	return &Cache{data: make(map[string]*Object)}
}

func (c *Cache) Set(key, value string) {
	c.data[key] = &Object{
		Type:  StringType,
		Value: value,
	}
}

func (c *Cache) Get(key string) (string, bool, error) {
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
	_, exists := c.data[key]
	if exists {
		delete(c.data, key)
	}
	return exists
}
