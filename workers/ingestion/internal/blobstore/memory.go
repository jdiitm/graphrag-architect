package blobstore

import (
	"context"
	"fmt"
	"sync"
)

type InMemoryBlobStore struct {
	mu    sync.RWMutex
	store map[string][]byte
}

func NewInMemoryBlobStore() *InMemoryBlobStore {
	return &InMemoryBlobStore{
		store: make(map[string][]byte),
	}
}

func (s *InMemoryBlobStore) Put(_ context.Context, key string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]byte, len(data))
	copy(copied, data)
	s.store[key] = copied
	return key, nil
}

func (s *InMemoryBlobStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.store[key]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s", key)
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

func (s *InMemoryBlobStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.store[key]
	return ok, nil
}
