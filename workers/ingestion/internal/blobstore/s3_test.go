package blobstore_test

import (
	"context"
	"testing"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/blobstore"
)

func TestS3BlobStore_PutAndGet(t *testing.T) {
	ctx := context.Background()
	mock := blobstore.NewMockS3Client()
	store := blobstore.NewS3BlobStore(mock, "test-bucket")

	key, err := store.Put(ctx, "ingestion/abc/file.go", []byte("package main"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if key != "ingestion/abc/file.go" {
		t.Errorf("expected key 'ingestion/abc/file.go', got %q", key)
	}

	data, err := store.Get(ctx, "ingestion/abc/file.go")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "package main" {
		t.Errorf("expected 'package main', got %q", string(data))
	}
}

func TestS3BlobStore_GetMissing(t *testing.T) {
	ctx := context.Background()
	mock := blobstore.NewMockS3Client()
	store := blobstore.NewS3BlobStore(mock, "test-bucket")

	_, err := store.Get(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestS3BlobStore_Exists(t *testing.T) {
	ctx := context.Background()
	mock := blobstore.NewMockS3Client()
	store := blobstore.NewS3BlobStore(mock, "test-bucket")

	exists, err := store.Exists(ctx, "key1")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected key1 to not exist")
	}

	_, _ = store.Put(ctx, "key1", []byte("data"))
	exists, err = store.Exists(ctx, "key1")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected key1 to exist after put")
	}
}

func TestS3BlobStore_IsolatesSlices(t *testing.T) {
	ctx := context.Background()
	mock := blobstore.NewMockS3Client()
	store := blobstore.NewS3BlobStore(mock, "test-bucket")

	original := []byte("hello")
	_, _ = store.Put(ctx, "k", original)
	original[0] = 'X'

	data, _ := store.Get(ctx, "k")
	if string(data) != "hello" {
		t.Error("store did not copy on put; mutation leaked")
	}

	data[0] = 'Z'
	data2, _ := store.Get(ctx, "k")
	if string(data2) != "hello" {
		t.Error("store did not copy on get; mutation leaked")
	}
}

func TestS3BlobStore_PutError(t *testing.T) {
	ctx := context.Background()
	mock := blobstore.NewMockS3Client()
	mock.SetPutError("forced put error")
	store := blobstore.NewS3BlobStore(mock, "test-bucket")

	_, err := store.Put(ctx, "key", []byte("data"))
	if err == nil {
		t.Fatal("expected error from put")
	}
}

func TestS3BlobStore_GetError(t *testing.T) {
	ctx := context.Background()
	mock := blobstore.NewMockS3Client()
	store := blobstore.NewS3BlobStore(mock, "test-bucket")

	_, _ = store.Put(ctx, "key", []byte("data"))
	mock.SetGetError("forced get error")

	_, err := store.Get(ctx, "key")
	if err == nil {
		t.Fatal("expected error from get")
	}
}

func TestS3BlobStore_CorrectBucket(t *testing.T) {
	ctx := context.Background()
	mock := blobstore.NewMockS3Client()
	store := blobstore.NewS3BlobStore(mock, "my-bucket")

	_, _ = store.Put(ctx, "k", []byte("data"))
	if mock.LastBucket() != "my-bucket" {
		t.Errorf("expected bucket 'my-bucket', got %q", mock.LastBucket())
	}
}

func TestS3BlobStore_ExistsError(t *testing.T) {
	ctx := context.Background()
	mock := blobstore.NewMockS3Client()
	mock.SetHeadError("forced head error")
	store := blobstore.NewS3BlobStore(mock, "test-bucket")

	exists, err := store.Exists(ctx, "any-key")
	if err == nil {
		t.Fatal("expected error from Exists when HeadObject fails")
	}
	if exists {
		t.Error("expected exists=false when HeadObject fails")
	}
}

func TestNewBlobStore_Memory(t *testing.T) {
	store, err := blobstore.NewBlobStoreFromEnv("memory", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store for memory type")
	}
	ctx := context.Background()
	_, err = store.Put(ctx, "k", []byte("v"))
	if err != nil {
		t.Fatalf("memory store put failed: %v", err)
	}
}

func TestNewBlobStore_S3EmptyBucket(t *testing.T) {
	_, err := blobstore.NewBlobStoreFromEnv("s3", "", "us-east-1")
	if err == nil {
		t.Fatal("expected error for empty bucket with s3 store type")
	}
}
