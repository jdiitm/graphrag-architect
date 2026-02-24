package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/ratelimit"
)

func TestTokenBucket_AllowUpToCapacity(t *testing.T) {
	tb := ratelimit.NewTokenBucket(10, 3)

	for i := 0; i < 3; i++ {
		if !tb.Allow() {
			t.Fatalf("Allow() returned false on token %d, expected true", i+1)
		}
	}
}

func TestTokenBucket_DeniesWhenEmpty(t *testing.T) {
	tb := ratelimit.NewTokenBucket(10, 2)

	tb.Allow()
	tb.Allow()

	if tb.Allow() {
		t.Fatal("Allow() returned true when bucket should be empty")
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	tb := ratelimit.NewTokenBucket(1000, 1)

	if !tb.Allow() {
		t.Fatal("first Allow() should succeed")
	}
	if tb.Allow() {
		t.Fatal("second Allow() should fail immediately")
	}

	time.Sleep(5 * time.Millisecond)

	if !tb.Allow() {
		t.Fatal("Allow() should succeed after refill period")
	}
}

func TestTokenBucket_WaitUnblocksWhenTokenAvailable(t *testing.T) {
	tb := ratelimit.NewTokenBucket(1000, 1)
	tb.Allow()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := tb.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() returned error: %v", err)
	}
}

func TestTokenBucket_WaitRespectsContextCancellation(t *testing.T) {
	tb := ratelimit.NewTokenBucket(0.001, 1)
	tb.Allow()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tb.Wait(ctx)
	if err == nil {
		t.Fatal("Wait() should return error when context is cancelled")
	}
}
