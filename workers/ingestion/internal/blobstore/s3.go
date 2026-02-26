package blobstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

type ObjectStorageClient interface {
	PutObject(ctx context.Context, bucket, key string, data []byte) error
	GetObject(ctx context.Context, bucket, key string) ([]byte, error)
	HeadObject(ctx context.Context, bucket, key string) (bool, error)
}

type S3BlobStore struct {
	client ObjectStorageClient
	bucket string
}

func NewS3BlobStore(client ObjectStorageClient, bucket string) *S3BlobStore {
	return &S3BlobStore{client: client, bucket: bucket}
}

func (s *S3BlobStore) Put(ctx context.Context, key string, data []byte) (string, error) {
	copied := make([]byte, len(data))
	copy(copied, data)
	if err := s.client.PutObject(ctx, s.bucket, key, copied); err != nil {
		return "", fmt.Errorf("s3 put %s/%s: %w", s.bucket, key, err)
	}
	return key, nil
}

func (s *S3BlobStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.client.GetObject(ctx, s.bucket, key)
	if err != nil {
		return nil, fmt.Errorf("s3 get %s/%s: %w", s.bucket, key, err)
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

func (s *S3BlobStore) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := s.client.HeadObject(ctx, s.bucket, key)
	if err != nil {
		return false, fmt.Errorf("s3 head %s/%s: %w", s.bucket, key, err)
	}
	return exists, nil
}

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

var (
	_ io.Reader    = (*bytes.Reader)(nil)
	_ BlobStore    = (*S3BlobStore)(nil)
	_ BlobStore    = (*InMemoryBlobStore)(nil)
)
