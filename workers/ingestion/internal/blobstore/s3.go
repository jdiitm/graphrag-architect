package blobstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

var (
	_ io.Reader    = (*bytes.Reader)(nil)
	_ BlobStore    = (*S3BlobStore)(nil)
	_ BlobStore    = (*InMemoryBlobStore)(nil)
)
