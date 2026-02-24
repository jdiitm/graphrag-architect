package outbox_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/outbox"
)

func TestFileOutbox_StoreCreatesRecord(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	rec := outbox.Record{
		ID:          "rec-001",
		StagingPath: "/data/staging/cmd/main.go",
		Topic:       "extraction-pending",
		Key:         []byte("cmd/main.go"),
		Value:       []byte(`{"staging_path":"/data/staging/cmd/main.go"}`),
	}

	if err := ob.Store(rec); err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}
}

func TestFileOutbox_StoreAndRetrievePending(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	rec := outbox.Record{
		ID:          "rec-002",
		StagingPath: "/data/staging/pkg/util.go",
		Topic:       "extraction-pending",
		Key:         []byte("pkg/util.go"),
		Value:       []byte(`{"staging_path":"/data/staging/pkg/util.go"}`),
	}

	if err := ob.Store(rec); err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	pending, err := ob.PendingOlderThan(0)
	if err != nil {
		t.Fatalf("PendingOlderThan() error = %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}
	if pending[0].ID != "rec-002" {
		t.Errorf("ID = %q, want rec-002", pending[0].ID)
	}
	if pending[0].StagingPath != "/data/staging/pkg/util.go" {
		t.Errorf("StagingPath = %q, want /data/staging/pkg/util.go", pending[0].StagingPath)
	}
}

func TestFileOutbox_MarkEmittedExcludesFromPending(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	rec := outbox.Record{
		ID:          "rec-003",
		StagingPath: "/data/staging/svc/app.go",
		Topic:       "extraction-pending",
		Key:         []byte("svc/app.go"),
		Value:       []byte(`{"staging_path":"/data/staging/svc/app.go"}`),
	}

	if err := ob.Store(rec); err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	if err := ob.MarkEmitted("rec-003"); err != nil {
		t.Fatalf("MarkEmitted() error = %v", err)
	}

	pending, err := ob.PendingOlderThan(0)
	if err != nil {
		t.Fatalf("PendingOlderThan() error = %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after mark, got %d", len(pending))
	}
}

func TestFileOutbox_RemoveDeletesRecord(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	rec := outbox.Record{
		ID:          "rec-004",
		StagingPath: "/data/staging/test.go",
		Topic:       "extraction-pending",
		Key:         []byte("test.go"),
		Value:       []byte("{}"),
	}

	if err := ob.Store(rec); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	if err := ob.Remove("rec-004"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 files after remove, got %d", len(entries))
	}
}

func TestFileOutbox_PendingOlderThanRespectsAge(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	rec := outbox.Record{
		ID:          "rec-005",
		StagingPath: "/data/staging/new.go",
		Topic:       "extraction-pending",
		Key:         []byte("new.go"),
		Value:       []byte("{}"),
	}

	if err := ob.Store(rec); err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	stale, err := ob.PendingOlderThan(10 * time.Second)
	if err != nil {
		t.Fatalf("PendingOlderThan() error = %v", err)
	}
	if len(stale) != 0 {
		t.Errorf("brand-new record should not be stale, got %d", len(stale))
	}
}

func TestFileOutbox_DuplicateStoreReturnsError(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	rec := outbox.Record{
		ID:          "dup-001",
		StagingPath: "/data/staging/dup.go",
		Topic:       "extraction-pending",
		Key:         []byte("dup.go"),
		Value:       []byte("{}"),
	}

	if err := ob.Store(rec); err != nil {
		t.Fatalf("first Store() error = %v", err)
	}
	if err := ob.Store(rec); err == nil {
		t.Fatal("expected error on duplicate Store(), got nil")
	}
}

func TestFileOutbox_MarkEmittedNonexistentReturnsError(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	if err := ob.MarkEmitted("nonexistent"); err == nil {
		t.Fatal("expected error marking nonexistent record")
	}
}

func TestReaper_ReEmitsStaleRecords(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	rec := outbox.Record{
		ID:          "stale-001",
		StagingPath: "/data/staging/stale.go",
		Topic:       "extraction-pending",
		Key:         []byte("stale.go"),
		Value:       []byte(`{"staging_path":"/data/staging/stale.go"}`),
	}
	if err := ob.Store(rec); err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	backdated := filepath.Join(dir, "stale-001.json")
	oldTime := time.Now().Add(-60 * time.Second)
	if err := os.Chtimes(backdated, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	emitter := &fakeEmitter{}
	reaper := outbox.NewReaper(ob, emitter, outbox.WithStaleThreshold(30*time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reaped, err := reaper.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce() error = %v", err)
	}
	if reaped != 1 {
		t.Errorf("expected 1 reaped, got %d", reaped)
	}
	if len(emitter.emitted) != 1 {
		t.Fatalf("expected 1 re-emission, got %d", len(emitter.emitted))
	}
	if emitter.emitted[0].topic != "extraction-pending" {
		t.Errorf("topic = %q, want extraction-pending", emitter.emitted[0].topic)
	}
}

func TestReaper_SkipsFreshRecords(t *testing.T) {
	dir := t.TempDir()
	ob := outbox.NewFileOutbox(dir)

	rec := outbox.Record{
		ID:          "fresh-001",
		StagingPath: "/data/staging/fresh.go",
		Topic:       "extraction-pending",
		Key:         []byte("fresh.go"),
		Value:       []byte("{}"),
	}
	if err := ob.Store(rec); err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	emitter := &fakeEmitter{}
	reaper := outbox.NewReaper(ob, emitter, outbox.WithStaleThreshold(30*time.Second))

	ctx := context.Background()
	reaped, err := reaper.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce() error = %v", err)
	}
	if reaped != 0 {
		t.Errorf("expected 0 reaped for fresh records, got %d", reaped)
	}
}

type fakeEmitter struct {
	emitted []emittedRecord
}

type emittedRecord struct {
	topic string
	key   []byte
	value []byte
}

func (f *fakeEmitter) Emit(_ context.Context, topic string, key []byte, value []byte) error {
	f.emitted = append(f.emitted, emittedRecord{topic: topic, key: key, value: value})
	return nil
}
