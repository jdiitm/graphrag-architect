package dedup

import (
	"fmt"
	"sync"
	"time"
)

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
