package dedup

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type RedisClient interface {
	SetNX(key string, ttl time.Duration) (bool, error)
	Exists(key string) (bool, error)
}

type RedisStore struct {
	client RedisClient
	prefix string
	ttl    time.Duration
}

func NewRedisStore(client RedisClient, prefix string, ttl time.Duration) *RedisStore {
	return &RedisStore{client: client, prefix: prefix, ttl: ttl}
}

func (r *RedisStore) IsDuplicate(key string) bool {
	if key == "" {
		return false
	}
	exists, err := r.client.Exists(r.prefix + key)
	if err != nil {
		log.Printf("dedup redis exists error key=%s: %v", key, err)
		return false
	}
	return exists
}

func (r *RedisStore) Mark(key string) {
	if key == "" {
		return
	}
	_, err := r.client.SetNX(r.prefix+key, r.ttl)
	if err != nil {
		log.Printf("dedup redis setnx error key=%s: %v", key, err)
	}
}

type MockRedisClient struct {
	mu      sync.Mutex
	store   map[string]time.Duration
	errMsg  string
	lastTTL time.Duration
}

func NewMockRedisClient() *MockRedisClient {
	return &MockRedisClient{store: make(map[string]time.Duration)}
}

func (m *MockRedisClient) SetError(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errMsg = msg
}

func (m *MockRedisClient) KeyCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.store)
}

func (m *MockRedisClient) HasKey(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.store[key]
	return ok
}

func (m *MockRedisClient) LastTTL() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTTL
}

func (m *MockRedisClient) SetNX(key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.errMsg != "" {
		return false, fmt.Errorf("%s", m.errMsg)
	}
	if _, exists := m.store[key]; exists {
		return false, nil
	}
	m.store[key] = ttl
	m.lastTTL = ttl
	return true, nil
}

func (m *MockRedisClient) Exists(key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.errMsg != "" {
		return false, fmt.Errorf("%s", m.errMsg)
	}
	_, ok := m.store[key]
	return ok, nil
}
