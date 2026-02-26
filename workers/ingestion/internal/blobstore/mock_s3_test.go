package blobstore

import (
	"context"
	"fmt"
	"sync"
)

type MockS3Client struct {
	mu         sync.RWMutex
	objects    map[string][]byte
	lastBucket string
	putErr     string
	getErr     string
	headErr    string
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{objects: make(map[string][]byte)}
}

func (m *MockS3Client) SetPutError(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putErr = msg
}

func (m *MockS3Client) SetGetError(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getErr = msg
}

func (m *MockS3Client) SetHeadError(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.headErr = msg
}

func (m *MockS3Client) LastBucket() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastBucket
}

func (m *MockS3Client) PutObject(_ context.Context, bucket, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putErr != "" {
		return fmt.Errorf("%s", m.putErr)
	}
	m.lastBucket = bucket
	copied := make([]byte, len(data))
	copy(copied, data)
	m.objects[bucket+"/"+key] = copied
	return nil
}

func (m *MockS3Client) GetObject(_ context.Context, bucket, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.getErr != "" {
		return nil, fmt.Errorf("%s", m.getErr)
	}
	data, ok := m.objects[bucket+"/"+key]
	if !ok {
		return nil, fmt.Errorf("NoSuchKey: %s/%s", bucket, key)
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

func (m *MockS3Client) HeadObject(_ context.Context, bucket, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.headErr != "" {
		return false, fmt.Errorf("%s", m.headErr)
	}
	_, ok := m.objects[bucket+"/"+key]
	return ok, nil
}
