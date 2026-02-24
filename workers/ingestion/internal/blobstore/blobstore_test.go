package blobstore_test

import (
	"context"
	"testing"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/blobstore"
)

func TestInMemoryBlobStore_PutAndGet(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewInMemoryBlobStore()

	key, err := store.Put(ctx, "test/file.go", []byte("package main"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if key != "test/file.go" {
		t.Errorf("expected key 'test/file.go', got %q", key)
	}

	data, err := store.Get(ctx, "test/file.go")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "package main" {
		t.Errorf("expected 'package main', got %q", string(data))
	}
}

func TestInMemoryBlobStore_GetMissing(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewInMemoryBlobStore()

	_, err := store.Get(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestInMemoryBlobStore_Exists(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewInMemoryBlobStore()

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

func TestInMemoryBlobStore_IsolatesSlices(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewInMemoryBlobStore()

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
