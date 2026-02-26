package dedup_test

import (
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dedup"
)

func TestRedisStore_MarkAndIsDuplicate(t *testing.T) {
	mock := dedup.NewMockRedisClient()
	store := dedup.NewRedisStore(mock, "dedup:", 7*24*time.Hour)

	if store.IsDuplicate("abc123:src/main.go") {
		t.Error("expected key to not be duplicate before marking")
	}

	store.Mark("abc123:src/main.go")

	if !store.IsDuplicate("abc123:src/main.go") {
		t.Error("expected key to be duplicate after marking")
	}
}

func TestRedisStore_EmptyKey(t *testing.T) {
	mock := dedup.NewMockRedisClient()
	store := dedup.NewRedisStore(mock, "dedup:", 7*24*time.Hour)

	if store.IsDuplicate("") {
		t.Error("empty key should never be duplicate")
	}

	store.Mark("")
	if mock.KeyCount() != 0 {
		t.Error("empty key should not be stored")
	}
}

func TestRedisStore_KeyPrefix(t *testing.T) {
	mock := dedup.NewMockRedisClient()
	store := dedup.NewRedisStore(mock, "myprefix:", 7*24*time.Hour)

	store.Mark("file1")
	if !mock.HasKey("myprefix:file1") {
		t.Error("expected prefixed key 'myprefix:file1' in store")
	}
}

func TestRedisStore_TTLApplied(t *testing.T) {
	mock := dedup.NewMockRedisClient()
	ttl := 24 * time.Hour
	store := dedup.NewRedisStore(mock, "dedup:", ttl)

	store.Mark("key1")
	if mock.LastTTL() != ttl {
		t.Errorf("expected TTL %v, got %v", ttl, mock.LastTTL())
	}
}

func TestRedisStore_DifferentKeysSeparate(t *testing.T) {
	mock := dedup.NewMockRedisClient()
	store := dedup.NewRedisStore(mock, "dedup:", time.Hour)

	store.Mark("key1")
	store.Mark("key2")

	if !store.IsDuplicate("key1") {
		t.Error("key1 should be duplicate")
	}
	if !store.IsDuplicate("key2") {
		t.Error("key2 should be duplicate")
	}
	if store.IsDuplicate("key3") {
		t.Error("key3 should not be duplicate")
	}
}

func TestRedisStore_SetNXFailure(t *testing.T) {
	mock := dedup.NewMockRedisClient()
	store := dedup.NewRedisStore(mock, "dedup:", time.Hour)

	mock.SetError("connection refused")
	store.Mark("key1")

	if store.IsDuplicate("key1") {
		t.Error("key should not be duplicate when redis errored on mark")
	}
}

func TestRedisStore_GetFailure(t *testing.T) {
	mock := dedup.NewMockRedisClient()
	store := dedup.NewRedisStore(mock, "dedup:", time.Hour)

	store.Mark("key1")
	mock.SetError("connection refused")

	if store.IsDuplicate("key1") {
		t.Error("should return false when redis errors on check")
	}
}

func TestRedisStore_ImplementsStoreInterface(t *testing.T) {
	mock := dedup.NewMockRedisClient()
	var _ dedup.Store = dedup.NewRedisStore(mock, "dedup:", time.Hour)
}

func TestNewDedupStoreFromEnv_Noop(t *testing.T) {
	store := dedup.NewStoreFromEnv("noop", "", 0, 0)
	if store.IsDuplicate("anything") {
		t.Error("noop store should never report duplicates")
	}
}

func TestNewDedupStoreFromEnv_Memory(t *testing.T) {
	store := dedup.NewStoreFromEnv("memory", "", 100, 0)
	store.Mark("x")
	if !store.IsDuplicate("x") {
		t.Error("memory store should detect duplicate")
	}
}
