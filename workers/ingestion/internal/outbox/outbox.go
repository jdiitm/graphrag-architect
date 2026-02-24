package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusEmitted Status = "emitted"
)

type Record struct {
	ID          string `json:"id"`
	StagingPath string `json:"staging_path"`
	Topic       string `json:"topic"`
	Key         []byte `json:"key"`
	Value       []byte `json:"value"`
	Status      Status `json:"status"`
	CreatedAt   int64  `json:"created_at"`
}

type Outbox interface {
	Store(rec Record) error
	MarkEmitted(id string) error
	PendingOlderThan(age time.Duration) ([]Record, error)
	Remove(id string) error
}

type FileOutbox struct {
	dir string
	mu  sync.Mutex
}

func NewFileOutbox(dir string) *FileOutbox {
	return &FileOutbox{dir: dir}
}

func (f *FileOutbox) path(id string) string {
	return filepath.Join(f.dir, id+".json")
}

func (f *FileOutbox) Store(rec Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	p := f.path(rec.ID)
	if _, err := os.Stat(p); err == nil {
		return fmt.Errorf("outbox record %q already exists", rec.ID)
	}

	rec.Status = StatusPending
	rec.CreatedAt = time.Now().UnixNano()

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal outbox record: %w", err)
	}
	return os.WriteFile(p, data, 0o644)
}

func (f *FileOutbox) MarkEmitted(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	p := f.path(id)
	data, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("read outbox record %q: %w", id, err)
	}

	var rec Record
	if err := json.Unmarshal(data, &rec); err != nil {
		return fmt.Errorf("unmarshal outbox record: %w", err)
	}

	rec.Status = StatusEmitted
	updated, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal updated record: %w", err)
	}
	return os.WriteFile(p, updated, 0o644)
}

func (f *FileOutbox) PendingOlderThan(age time.Duration) ([]Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, fmt.Errorf("read outbox directory: %w", err)
	}

	cutoff := time.Now().Add(-age)
	var result []Record

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}

		info, err := e.Info()
		if err != nil {
			continue
		}

		data, err := os.ReadFile(filepath.Join(f.dir, e.Name()))
		if err != nil {
			continue
		}

		var rec Record
		if err := json.Unmarshal(data, &rec); err != nil {
			continue
		}

		if rec.Status != StatusPending {
			continue
		}

		if info.ModTime().After(cutoff) {
			continue
		}

		result = append(result, rec)
	}

	return result, nil
}

func (f *FileOutbox) Remove(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return os.Remove(f.path(id))
}

type EventEmitter interface {
	Emit(ctx context.Context, topic string, key []byte, value []byte) error
}

type ReaperConfig struct {
	StaleThreshold time.Duration
}

type ReaperOption func(*ReaperConfig)

func WithStaleThreshold(d time.Duration) ReaperOption {
	return func(c *ReaperConfig) {
		c.StaleThreshold = d
	}
}

type Reaper struct {
	outbox  Outbox
	emitter EventEmitter
	cfg     ReaperConfig
}

func NewReaper(ob Outbox, emitter EventEmitter, opts ...ReaperOption) *Reaper {
	cfg := ReaperConfig{StaleThreshold: 30 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}
	return &Reaper{outbox: ob, emitter: emitter, cfg: cfg}
}

func (r *Reaper) ReapOnce(ctx context.Context) (int, error) {
	stale, err := r.outbox.PendingOlderThan(r.cfg.StaleThreshold)
	if err != nil {
		return 0, fmt.Errorf("query stale records: %w", err)
	}

	reaped := 0
	for _, rec := range stale {
		if ctx.Err() != nil {
			return reaped, ctx.Err()
		}
		if err := r.emitter.Emit(ctx, rec.Topic, rec.Key, rec.Value); err != nil {
			continue
		}
		if err := r.outbox.MarkEmitted(rec.ID); err != nil {
			continue
		}
		reaped++
	}
	return reaped, nil
}
