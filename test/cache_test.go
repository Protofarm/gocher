package cache_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Protofarm/gocher"
)

func mustGet(t *testing.T, c *gocher.Cache, key string) []byte {
	t.Helper()
	v, ok := c.Get(key)
	if !ok {
		t.Fatalf("expected key %q to exist", key)
	}
	return v
}

func TestNewCache(t *testing.T) {
	c := gocher.NewCache()
	if c == nil {
		t.Fatal("NewCache returned nil")
	}
}

func TestNewCachePreallocatedKeysNotVisible(t *testing.T) {
	keys := []string{"a", "b", "c"}
	c := gocher.NewCache(keys...)
	for _, k := range keys {
		_, ok := c.Get(k)
		if ok {
			t.Errorf("pre-allocated key %q should not be found before Set", k)
		}
	}
}

func TestNewCachePreallocatedKeyAfterSet(t *testing.T) {
	c := gocher.NewCache("prealloc")
	c.Set("prealloc", []byte("val"), 0)
	v, ok := c.Get("prealloc")
	if !ok {
		t.Fatal("pre-allocated key not found after Set")
	}
	if string(v) != "val" {
		t.Errorf("got %q, want %q", string(v), "val")
	}
}

func TestNewCachePreallocatedAndDynamicKeys(t *testing.T) {
	c := gocher.NewCache("pre1", "pre2")
	c.Set("pre1", []byte("v1"), 0)
	c.Set("pre2", []byte("v2"), 0)
	c.Set("dynamic", []byte("v3"), 0)

	cases := map[string]string{"pre1": "v1", "pre2": "v2", "dynamic": "v3"}
	for key, want := range cases {
		v, ok := c.Get(key)
		if !ok {
			t.Errorf("key %q not found", key)
			continue
		}
		if string(v) != want {
			t.Errorf("key %q: got %q, want %q", key, string(v), want)
		}
	}
}

func TestGetMissingKey(t *testing.T) {
	c := gocher.NewCache()
	v, ok := c.Get("missing")
	if ok {
		t.Error("expected ok=false for missing key")
	}
	if v != nil {
		t.Errorf("expected nil value for missing key, got %v", v)
	}
}

func TestSetAndGet(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte("value"), 0)
	v, ok := c.Get("key")
	if !ok {
		t.Fatal("expected key to be found")
	}
	if string(v) != "value" {
		t.Errorf("got %q, want %q", string(v), "value")
	}
}

func TestSetOverwritesValue(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte("first"), 0)
	c.Set("key", []byte("second"), 0)
	v := mustGet(t, c, "key")
	if string(v) != "second" {
		t.Errorf("got %q, want %q", string(v), "second")
	}
}

func TestSetMultipleOverwrites(t *testing.T) {
	c := gocher.NewCache()
	for i := 0; i < 10; i++ {
		c.Set("key", []byte(fmt.Sprintf("val%d", i)), 0)
	}
	v := mustGet(t, c, "key")
	if string(v) != "val9" {
		t.Errorf("got %q, want %q", string(v), "val9")
	}
}

func TestSetEmptyValue(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte{}, 0)
	v, ok := c.Get("key")
	if !ok {
		t.Fatal("empty-value key should be found")
	}
	if len(v) != 0 {
		t.Errorf("expected empty value, got len=%d", len(v))
	}
}

func TestSetNilValue(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", nil, 0)
	v, ok := c.Get("key")
	if !ok {
		t.Fatal("nil-value key should be found")
	}
	if v != nil {
		t.Errorf("expected nil value, got %v", v)
	}
}

func TestIndependentKeys(t *testing.T) {
	c := gocher.NewCache()
	c.Set("a", []byte("alpha"), 0)
	c.Set("b", []byte("beta"), 0)
	if string(mustGet(t, c, "a")) != "alpha" {
		t.Error("key a has wrong value")
	}
	if string(mustGet(t, c, "b")) != "beta" {
		t.Error("key b has wrong value")
	}
}

func TestNeverExpiresWhenZero(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte("val"), 0)
	_, ok := c.Get("key")
	if !ok {
		t.Error("key with expires=0 should never expire")
	}
}

func TestFutureExpiry(t *testing.T) {
	c := gocher.NewCache()
	future := time.Now().Add(10 * time.Second).Unix()
	c.Set("key", []byte("val"), future)
	_, ok := c.Get("key")
	if !ok {
		t.Error("key with future expiry should be found")
	}
}

func TestPastExpiry(t *testing.T) {
	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("val"), past)
	_, ok := c.Get("key")
	if ok {
		t.Error("key with past expiry should not be found")
	}
}

func TestExpiredReturnsNilAndFalse(t *testing.T) {
	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("val"), past)
	v, ok := c.Get("key")
	if ok || v != nil {
		t.Errorf("expired key should return nil,false; got %v,%v", v, ok)
	}
}

func TestExpiryAtNow(t *testing.T) {
	c := gocher.NewCache()

	now := time.Now().Unix()
	c.Set("key", []byte("val"), now)
	time.Sleep(time.Millisecond)
	_, ok := c.Get("key")
	if ok {
		t.Error("key with expiry == now should be expired")
	}
}

func TestSetOverwriteResetsExpiry(t *testing.T) {
	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("old"), past)

	c.Set("key", []byte("new"), 0)
	v, ok := c.Get("key")
	if !ok {
		t.Fatal("key should be found after resetting to no-expiry")
	}
	if string(v) != "new" {
		t.Errorf("got %q, want %q", string(v), "new")
	}
}

func TestSetExtendsTTL(t *testing.T) {
	c := gocher.NewCache()
	near := time.Now().Add(1 * time.Second).Unix()
	c.Set("key", []byte("v1"), near)

	far := time.Now().Add(1000 * time.Second).Unix()
	c.Set("key", []byte("v2"), far)

	v, ok := c.Get("key")
	if !ok {
		t.Fatal("key with extended TTL should be found")
	}
	if string(v) != "v2" {
		t.Errorf("got %q, want %q", string(v), "v2")
	}
}

func TestGetWithVersionMissingKey(t *testing.T) {
	c := gocher.NewCache()
	v, ver, ok := c.GetWithVersion("missing")
	if ok || v != nil || ver != 0 {
		t.Errorf("missing key: got (%v, %d, %v), want (nil, 0, false)", v, ver, ok)
	}
}

func TestGetWithVersionPreallocatedNoSet(t *testing.T) {
	c := gocher.NewCache("key")
	_, ver, ok := c.GetWithVersion("key")
	if ok {
		t.Error("pre-allocated key without Set should not be found")
	}
	if ver != 0 {
		t.Errorf("expected version 0, got %d", ver)
	}
}

func TestFirstSetCreatesVersion1(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte("v"), 0)
	_, ver, ok := c.GetWithVersion("key")
	if !ok {
		t.Fatal("key not found")
	}
	if ver != 1 {
		t.Errorf("expected version 1 after first Set, got %d", ver)
	}
}

func TestVersionIncrementsOnEachSet(t *testing.T) {
	c := gocher.NewCache()
	for i := uint64(1); i <= 10; i++ {
		c.Set("key", []byte("v"), 0)
		_, ver, ok := c.GetWithVersion("key")
		if !ok {
			t.Fatalf("iteration %d: key not found", i)
		}
		if ver != i {
			t.Errorf("iteration %d: expected version %d, got %d", i, i, ver)
		}
	}
}

func TestGetWithVersionExpiredKey(t *testing.T) {
	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("v"), past)
	_, ver, ok := c.GetWithVersion("key")
	if ok {
		t.Error("expired key should not be found via GetWithVersion")
	}
	if ver != 0 {
		t.Errorf("expired key: expected version 0, got %d", ver)
	}
}

func TestSetWithVersionNewKeyExpectedZero(t *testing.T) {
	c := gocher.NewCache()
	ok := c.SetWithVersion("key", []byte("val"), 0, 0)
	if !ok {
		t.Fatal("SetWithVersion(expected=0) on new key should succeed")
	}
	_, ver, found := c.GetWithVersion("key")
	if !found {
		t.Fatal("key should exist after SetWithVersion")
	}
	if ver != 1 {
		t.Errorf("expected version 1, got %d", ver)
	}
}

func TestSetWithVersionNewKeyWrongExpected(t *testing.T) {
	c := gocher.NewCache()
	ok := c.SetWithVersion("key", []byte("val"), 1, 0)
	if ok {
		t.Error("SetWithVersion(expected=1) on new key should fail")
	}
	_, found := c.Get("key")
	if found {
		t.Error("key should not exist after failed SetWithVersion")
	}
}

func TestSetWithVersionCorrectVersion(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte("v1"), 0)
	_, ver, _ := c.GetWithVersion("key")

	ok := c.SetWithVersion("key", []byte("v2"), ver, 0)
	if !ok {
		t.Fatal("SetWithVersion with correct version should succeed")
	}
	v, newVer, found := c.GetWithVersion("key")
	if !found {
		t.Fatal("key should exist")
	}
	if string(v) != "v2" {
		t.Errorf("got %q, want %q", string(v), "v2")
	}
	if newVer != ver+1 {
		t.Errorf("expected version %d, got %d", ver+1, newVer)
	}
}

func TestSetWithVersionWrongVersion(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte("v1"), 0)

	ok := c.SetWithVersion("key", []byte("v2"), 99, 0)
	if ok {
		t.Error("SetWithVersion with wrong version should fail")
	}
	v := mustGet(t, c, "key")
	if string(v) != "v1" {
		t.Errorf("value should be unchanged, got %q", string(v))
	}
}

func TestSetWithVersionChain(t *testing.T) {
	c := gocher.NewCache()

	if !c.SetWithVersion("key", []byte("v1"), 0, 0) {
		t.Fatal("step 1 failed")
	}
	if !c.SetWithVersion("key", []byte("v2"), 1, 0) {
		t.Fatal("step 2 failed")
	}
	if !c.SetWithVersion("key", []byte("v3"), 2, 0) {
		t.Fatal("step 3 failed")
	}

	v, ver, found := c.GetWithVersion("key")
	if !found {
		t.Fatal("key not found")
	}
	if string(v) != "v3" {
		t.Errorf("got %q, want v3", string(v))
	}
	if ver != 3 {
		t.Errorf("expected version 3, got %d", ver)
	}
}

func TestSetWithVersionDoesNotModifyValueOnFailure(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte("original"), 0)
	c.SetWithVersion("key", []byte("modified"), 99, 0)
	v := mustGet(t, c, "key")
	if string(v) != "original" {
		t.Errorf("value should be unchanged, got %q", string(v))
	}
}

func TestSetWithVersionWithExpiry(t *testing.T) {
	c := gocher.NewCache()
	future := time.Now().Add(10 * time.Second).Unix()
	ok := c.SetWithVersion("key", []byte("val"), 0, future)
	if !ok {
		t.Fatal("SetWithVersion with future expiry failed")
	}
	_, ok = c.Get("key")
	if !ok {
		t.Error("key with future expiry should be found")
	}
}

func TestSetWithVersionExpiresEntry(t *testing.T) {
	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	ok := c.SetWithVersion("key", []byte("val"), 0, past)
	if !ok {
		t.Fatal("SetWithVersion with past expiry should still succeed")
	}
	_, found := c.Get("key")
	if found {
		t.Error("entry with past expiry should not be found")
	}
}

func TestSetWithVersionChecksVersionEvenForExpired(t *testing.T) {
	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("v1"), past)

	ok := c.SetWithVersion("key", []byte("v2"), 0, 0)
	if ok {
		t.Error("SetWithVersion(expected=0) on expired entry with version=1 should fail")
	}

	ok = c.SetWithVersion("key", []byte("v2"), 1, 0)
	if !ok {
		t.Error("SetWithVersion(expected=1) on expired entry with version=1 should succeed")
	}
}

func TestCloseReturnsNil(t *testing.T) {
	c := gocher.NewCache()
	if err := c.Close(); err != nil {
		t.Errorf("Close should return nil error, got %v", err)
	}
}

func TestCloseMultipleTimes(t *testing.T) {
	c := gocher.NewCache()
	for i := 0; i < 5; i++ {
		if err := c.Close(); err != nil {
			t.Errorf("Close #%d returned error: %v", i+1, err)
		}
	}
}

func TestCacheUsableAfterClose(t *testing.T) {
	c := gocher.NewCache()
	_ = c.Close()
	c.Set("key", []byte("val"), 0)
	v, ok := c.Get("key")
	if !ok {
		t.Fatal("cache should still work after Close")
	}
	if string(v) != "val" {
		t.Errorf("got %q, want %q", string(v), "val")
	}
}

func TestEmptyStringKey(t *testing.T) {
	c := gocher.NewCache()
	c.Set("", []byte("empty-key-val"), 0)
	v, ok := c.Get("")
	if !ok {
		t.Fatal("empty string key should work")
	}
	if string(v) != "empty-key-val" {
		t.Errorf("got %q, want %q", string(v), "empty-key-val")
	}
}

func TestLargeValue(t *testing.T) {
	c := gocher.NewCache()
	large := make([]byte, 1<<20)
	for i := range large {
		large[i] = byte(i % 251)
	}
	c.Set("large", large, 0)
	v, ok := c.Get("large")
	if !ok {
		t.Fatal("large value key not found")
	}
	if len(v) != len(large) {
		t.Fatalf("got len %d, want %d", len(v), len(large))
	}
	for i := range v {
		if v[i] != large[i] {
			t.Errorf("byte mismatch at index %d: got %d, want %d", i, v[i], large[i])
			break
		}
	}
}

func TestVeryLongKey(t *testing.T) {
	c := gocher.NewCache()
	key := string(make([]byte, 10000))
	c.Set(key, []byte("val"), 0)
	v, ok := c.Get(key)
	if !ok {
		t.Fatal("very long key not found")
	}
	if string(v) != "val" {
		t.Errorf("got %q, want %q", string(v), "val")
	}
}

func TestSpecialCharactersInKey(t *testing.T) {
	c := gocher.NewCache()
	keys := []string{
		"key with spaces",
		"key\twith\ttabs",
		"key\nwith\nnewlines",
		"key/with/slashes",
		"key:with:colons",
		"unicode-键",
		"emoji-🔑",
		"null-\x00-byte",
	}
	for _, k := range keys {
		c.Set(k, []byte(k), 0)
		v, ok := c.Get(k)
		if !ok {
			t.Errorf("key %q not found", k)
			continue
		}
		if string(v) != k {
			t.Errorf("key %q: got %q", k, string(v))
		}
	}
}

func TestManyDistinctKeys(t *testing.T) {
	c := gocher.NewCache()
	const n = 10000
	for i := 0; i < n; i++ {
		c.Set(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)), 0)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		v, ok := c.Get(key)
		if !ok {
			t.Errorf("key %q not found", key)
			continue
		}
		want := fmt.Sprintf("val-%d", i)
		if string(v) != want {
			t.Errorf("key %q: got %q, want %q", key, string(v), want)
		}
	}
}

func TestBinaryValue(t *testing.T) {
	c := gocher.NewCache()
	val := []byte{0x00, 0xFF, 0x01, 0xFE, 0x80, 0x7F}
	c.Set("bin", val, 0)
	v := mustGet(t, c, "bin")
	if len(v) != len(val) {
		t.Fatalf("length mismatch: got %d, want %d", len(v), len(val))
	}
	for i := range v {
		if v[i] != val[i] {
			t.Errorf("byte %d: got %02x, want %02x", i, v[i], val[i])
		}
	}
}

func TestAllShardsReachable(t *testing.T) {
	c := gocher.NewCache()
	const n = 160
	for i := 0; i < n; i++ {
		c.Set(fmt.Sprintf("shard-test-%d", i), []byte("v"), 0)
	}
	found := 0
	for i := 0; i < n; i++ {
		if _, ok := c.Get(fmt.Sprintf("shard-test-%d", i)); ok {
			found++
		}
	}
	if found != n {
		t.Errorf("expected %d keys found, got %d", n, found)
	}
}

func TestRaceSameKey(t *testing.T) {
	c := gocher.NewCache()
	const goroutines = 100
	const iters = 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				c.Set("shared", []byte(fmt.Sprintf("%d-%d", id, i)), 0)
				c.Get("shared")
			}
		}(g)
	}
	wg.Wait()
}

func TestRaceDifferentKeys(t *testing.T) {
	c := gocher.NewCache()
	const goroutines = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			val := []byte(fmt.Sprintf("val-%d", id))
			c.Set(key, val, 0)
			v, ok := c.Get(key)
			if ok && string(v) != fmt.Sprintf("val-%d", id) {
				t.Errorf("goroutine %d: got %q, want val-%d", id, string(v), id)
			}
		}(g)
	}
	wg.Wait()
}

func TestRaceConcurrentSetWithVersion(t *testing.T) {
	c := gocher.NewCache()
	const goroutines = 100
	var wins atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.SetWithVersion("key", []byte("val"), 0, 0) {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Errorf("exactly one goroutine should win CAS on new key, got %d wins", wins.Load())
	}
}

func TestRaceConcurrentGetWithVersion(t *testing.T) {
	c := gocher.NewCache()
	c.Set("key", []byte("initial"), 0)
	const goroutines = 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ver, ok := c.GetWithVersion("key")
			if ok && (v == nil || ver == 0) {
				t.Errorf("inconsistent result: ok=true but v=%v ver=%d", v, ver)
			}
		}()
	}
	wg.Wait()
}

func TestRaceMixedOperations(t *testing.T) {
	c := gocher.NewCache()
	keys := []string{"a", "b", "c", "d", "e"}
	c.Set(keys[0], []byte("init"), 0)

	const goroutines = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := keys[id%len(keys)]
			switch id % 4 {
			case 0:
				c.Set(key, []byte(fmt.Sprintf("v%d", id)), 0)
			case 1:
				c.Get(key)
			case 2:
				c.GetWithVersion(key)
			case 3:
				_, ver, ok := c.GetWithVersion(key)
				if ok {
					c.SetWithVersion(key, []byte(fmt.Sprintf("cas%d", id)), ver, 0)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestRaceConcurrentExpiry(t *testing.T) {
	c := gocher.NewCache()
	future := time.Now().Add(10 * time.Second).Unix()
	past := time.Now().Add(-10 * time.Second).Unix()

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", id)
			if id%2 == 0 {
				c.Set(key, []byte("v"), future)
			} else {
				c.Set(key, []byte("v"), past)
			}
			c.Get(key)
		}(g)
	}
	wg.Wait()
}

func TestGetClearsExpiredEntryPtr(t *testing.T) {
	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("v1"), past)

	if c.SetWithVersion("key", []byte("x"), 0, 0) {
		t.Fatal("SetWithVersion(expected=0) must fail while expired entry still has version=1")
	}

	if _, ok := c.Get("key"); ok {
		t.Fatal("expired key must return false")
	}

	if !c.SetWithVersion("key", []byte("v2"), 0, 0) {
		t.Error("SetWithVersion(expected=0) must succeed after Get CAS-nils the expired entry")
	}
	_, ver, ok := c.GetWithVersion("key")
	if !ok {
		t.Fatal("key must exist after SetWithVersion on cleared slot")
	}
	if ver != 1 {
		t.Errorf("fresh slot must produce version 1, got %d", ver)
	}
}

func TestGetWithVersionClearsExpiredEntryPtr(t *testing.T) {
	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("v1"), past)

	if _, _, ok := c.GetWithVersion("key"); ok {
		t.Fatal("expired key must return false")
	}

	if !c.SetWithVersion("key", []byte("v2"), 0, 0) {
		t.Error("SetWithVersion(expected=0) must succeed after GetWithVersion clears expired entry")
	}
}

func TestGetAfterClearReturnsNotFound(t *testing.T) {

	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("v"), past)

	for i := 0; i < 5; i++ {
		if _, ok := c.Get("key"); ok {
			t.Errorf("call %d: nil-ptr key must return false", i+1)
		}
	}
}

func TestGetExpiredThenSetCreatesVersion1(t *testing.T) {

	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("v1"), past)
	c.Get("key")

	c.Set("key", []byte("v2"), 0)
	_, ver, ok := c.GetWithVersion("key")
	if !ok {
		t.Fatal("key must exist after Set on cleared slot")
	}
	if ver != 1 {
		t.Errorf("Set on nil ptr must produce version 1, got %d", ver)
	}
}

func TestGetExpiredClearsOnlyMatchingEntry(t *testing.T) {

	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("key", []byte("expired"), past)

	c.Set("key", []byte("valid"), 0)

	v, ok := c.Get("key")
	if !ok {
		t.Fatal("valid entry must be found")
	}
	if string(v) != "valid" {
		t.Errorf("got %q, want valid", string(v))
	}
}

func TestActiveTTLClearsPreallocatedExpiredKey(t *testing.T) {
	c := gocher.NewCache("active-ttl-key")
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("active-ttl-key", []byte("v"), past)

	time.Sleep(75 * time.Millisecond)

	if !c.SetWithVersion("active-ttl-key", []byte("new"), 0, 0) {
		t.Error("active TTL handler must have cleared the expired entry by now")
	}
}

func TestActiveTTLDoesNotAffectDynamicKeys(t *testing.T) {

	c := gocher.NewCache()
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("dynamic", []byte("v"), past)

	time.Sleep(75 * time.Millisecond)

	if c.SetWithVersion("dynamic", []byte("new"), 0, 0) {
		t.Error("dynamic key must NOT be cleared by handler; SetWithVersion(expected=0) must fail")
	}
	if !c.SetWithVersion("dynamic", []byte("new"), 1, 0) {
		t.Error("SetWithVersion(expected=1) must succeed — entry still carries version=1")
	}
}

func TestActiveTTLPreservesValidPreallocatedKey(t *testing.T) {
	c := gocher.NewCache("valid-key")
	future := time.Now().Add(100 * time.Second).Unix()
	c.Set("valid-key", []byte("alive"), future)

	time.Sleep(75 * time.Millisecond)

	v, ok := c.Get("valid-key")
	if !ok {
		t.Fatal("non-expired pre-allocated key must survive active TTL handler")
	}
	if string(v) != "alive" {
		t.Errorf("got %q, want alive", string(v))
	}
}

func TestActiveTTLMixedExpiryPreallocatedKeys(t *testing.T) {
	c := gocher.NewCache("ttl-expired", "ttl-valid")
	past := time.Now().Add(-1 * time.Second).Unix()
	future := time.Now().Add(100 * time.Second).Unix()
	c.Set("ttl-expired", []byte("old"), past)
	c.Set("ttl-valid", []byte("alive"), future)

	time.Sleep(75 * time.Millisecond)

	if !c.SetWithVersion("ttl-expired", []byte("renewed"), 0, 0) {
		t.Error("handler should have cleared expired entry; SetWithVersion(expected=0) must succeed")
	}
	if v, ok := c.Get("ttl-valid"); !ok || string(v) != "alive" {
		t.Errorf("valid key: got %q ok=%v, want alive/true", string(v), ok)
	}
}

func TestActiveTTLMultipleExpiredPreallocatedKeys(t *testing.T) {
	keys := make([]string, 30)
	for i := range keys {
		keys[i] = fmt.Sprintf("ttl-multi-%d", i)
	}
	c := gocher.NewCache(keys...)
	past := time.Now().Add(-1 * time.Second).Unix()
	for _, k := range keys {
		c.Set(k, []byte("v"), past)
	}

	time.Sleep(300 * time.Millisecond)

	cleared := 0
	for _, k := range keys {
		if c.SetWithVersion(k, []byte("renewed"), 0, 0) {
			cleared++
		}
	}
	if cleared != len(keys) {
		t.Errorf("handler cleared %d/%d keys; all must be cleared within 300 ms", cleared, len(keys))
	}
}

func TestActiveTTLNoHandlerOnEmptyCache(t *testing.T) {
	c := gocher.NewCache()
	time.Sleep(30 * time.Millisecond)

	c.Set("key", []byte("v"), 0)
	v, ok := c.Get("key")
	if !ok || string(v) != "v" {
		t.Error("cache must operate normally when all TTL handlers have exited")
	}
}

func TestRaceActiveTTLAndConcurrentSet(t *testing.T) {
	c := gocher.NewCache("race-ttl-key")
	past := time.Now().Add(-1 * time.Second).Unix()
	c.Set("race-ttl-key", []byte("initial"), past)

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.Set("race-ttl-key", []byte(fmt.Sprintf("v%d", id)), 0)
			c.Get("race-ttl-key")
		}(g)
	}
	wg.Wait()

	if _, ok := c.Get("race-ttl-key"); !ok {
		t.Error("key must exist after concurrent Sets overwrite the expired entry")
	}
}

func BenchmarkSet(b *testing.B) {
	c := gocher.NewCache()
	val := []byte("benchmark-value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("bench-key", val, 0)
	}
}

func BenchmarkGet(b *testing.B) {
	c := gocher.NewCache()
	c.Set("bench-key", []byte("benchmark-value"), 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("bench-key")
	}
}

func BenchmarkSetGet(b *testing.B) {
	c := gocher.NewCache()
	val := []byte("benchmark-value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("bench-key", val, 0)
		c.Get("bench-key")
	}
}

func BenchmarkGetWithVersion(b *testing.B) {
	c := gocher.NewCache()
	c.Set("bench-key", []byte("benchmark-value"), 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.GetWithVersion("bench-key")
	}
}

func BenchmarkSetWithVersion(b *testing.B) {
	c := gocher.NewCache()
	c.Set("bench-key", []byte("initial"), 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ver, _ := c.GetWithVersion("bench-key")
		c.SetWithVersion("bench-key", []byte("updated"), ver, 0)
	}
}

func BenchmarkPreallocatedSet(b *testing.B) {
	c := gocher.NewCache("bench-key")
	val := []byte("benchmark-value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("bench-key", val, 0)
	}
}

func BenchmarkConcurrentGet(b *testing.B) {
	c := gocher.NewCache()
	c.Set("bench-key", []byte("benchmark-value"), 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("bench-key")
		}
	})
}

func BenchmarkConcurrentSet(b *testing.B) {
	c := gocher.NewCache()
	val := []byte("benchmark-value")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Set("bench-key", val, 0)
		}
	})
}

func BenchmarkConcurrentMixedKeys(b *testing.B) {
	c := gocher.NewCache()
	val := []byte("v")
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	var idx atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1)) % len(keys)
			c.Set(keys[i], val, 0)
			c.Get(keys[i])
		}
	})
}

func BenchmarkConcurrentSetWithVersion(b *testing.B) {
	c := gocher.NewCache()
	c.Set("bench-key", []byte("initial"), 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ver, ok := c.GetWithVersion("bench-key")
			if ok {
				c.SetWithVersion("bench-key", []byte("updated"), ver, 0)
			}
		}
	})
}

func BenchmarkManyKeys(b *testing.B) {
	c := gocher.NewCache()
	val := []byte("v")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%d", i)
		c.Set(key, val, 0)
		c.Get(key)
	}
}
