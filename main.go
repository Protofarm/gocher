package gocher

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
