package blobstore

import "context"

type BlobStore interface {
	Put(ctx context.Context, key string, data []byte) (string, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Exists(ctx context.Context, key string) (bool, error)
}
