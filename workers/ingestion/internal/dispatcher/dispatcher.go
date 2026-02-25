package dispatcher

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dedup"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/metrics"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

type Config struct {
	NumWorkers     int
	MaxRetries     int
	JobBuffer      int
	DLQBuffer      int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	DLQAckTimeout  time.Duration
	JobTimeout     time.Duration
}

type Dispatcher struct {
	cfg       Config
	processor processor.DocumentProcessor
	observer  metrics.PipelineObserver
	dedup     dedup.Store
	jobs      chan domain.Job
	dlq       chan domain.Result
	acks      chan struct{}
	wg        sync.WaitGroup
}

func New(cfg Config, proc processor.DocumentProcessor, opts ...Option) *Dispatcher {
	d := &Dispatcher{
		cfg:       cfg,
		processor: proc,
		observer:  metrics.NoopObserver{},
		dedup:     dedup.NoopStore{},
		jobs:      make(chan domain.Job, cfg.JobBuffer),
		dlq:       make(chan domain.Result, cfg.DLQBuffer),
		acks:      make(chan struct{}, cfg.JobBuffer),
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

type Option func(*Dispatcher)

func WithObserver(obs metrics.PipelineObserver) Option {
	return func(d *Dispatcher) {
		d.observer = obs
	}
}

func WithDedup(store dedup.Store) Option {
	return func(d *Dispatcher) {
		d.dedup = store
	}
}

func (d *Dispatcher) Jobs() chan<- domain.Job {
	return d.jobs
}

func (d *Dispatcher) DLQ() <-chan domain.Result {
	return d.dlq
}

func (d *Dispatcher) Acks() <-chan struct{} {
	return d.acks
}

func (d *Dispatcher) Run(ctx context.Context) {
	for i := 0; i < d.cfg.NumWorkers; i++ {
		d.wg.Add(1)
		go d.worker(ctx)
	}
	d.wg.Wait()
}

func (d *Dispatcher) worker(ctx context.Context) {
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-d.jobs:
			if !ok {
				return
			}
			dedupKey := string(job.Key)
			if dedupKey == "" {
				dedupKey = fmt.Sprintf("%s:%d:%d", job.Topic, job.Partition, job.Offset)
			}
			if d.dedup.IsDuplicate(dedupKey) {
				d.observer.RecordJobProcessed("dedup_skipped")
				select {
				case d.acks <- struct{}{}:
				case <-ctx.Done():
					return
				}
				continue
			}
			jobCtx := telemetry.ExtractTraceContext(ctx, job.Headers)
			processCtx, processSpan := telemetry.StartProcessSpan(jobCtx, job)
			var jobCancel context.CancelFunc
			if d.cfg.JobTimeout > 0 {
				processCtx, jobCancel = context.WithTimeout(processCtx, d.cfg.JobTimeout)
			}
			result := d.processWithRetry(processCtx, job)
			if jobCancel != nil {
				jobCancel()
			}
			if result.Err != nil {
				_, dlqSpan := telemetry.StartDLQSpan(processCtx, result)
				result.Done = make(chan struct{})
				d.dlq <- result
				d.awaitDLQDone(ctx, result)
				dlqSpan.End()
				d.observer.RecordDLQRouted()
				d.observer.RecordJobProcessed("dlq")
			} else {
				d.dedup.Mark(dedupKey)
				d.observer.RecordJobProcessed("success")
			}
			processSpan.End()
			select {
			case d.acks <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (d *Dispatcher) awaitDLQDone(ctx context.Context, result domain.Result) {
	if d.cfg.DLQAckTimeout <= 0 {
		select {
		case <-result.Done:
		case <-ctx.Done():
		}
		return
	}
	timer := time.NewTimer(d.cfg.DLQAckTimeout)
	defer timer.Stop()
	select {
	case <-result.Done:
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (d *Dispatcher) processWithRetry(ctx context.Context, job domain.Job) domain.Result {
	var lastErr error
	for attempt := 1; attempt <= d.cfg.MaxRetries; attempt++ {
		lastErr = d.processor.Process(ctx, job)
		if lastErr == nil {
			return domain.Result{Job: job, Attempts: attempt}
		}
		if ctx.Err() != nil {
			break
		}
		if attempt < d.cfg.MaxRetries && d.cfg.BaseBackoff > 0 {
			delay := backoffWithJitter(d.cfg.BaseBackoff, d.cfg.MaxBackoff, attempt)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return domain.Result{Job: job, Err: lastErr, Attempts: attempt}
			}
		}
	}
	return domain.Result{Job: job, Err: lastErr, Attempts: d.cfg.MaxRetries}
}

func cryptoFloat64() float64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return 0.5
	}
	return float64(binary.LittleEndian.Uint64(b[:])>>11) / (1 << 53)
}

func backoffWithJitter(base, maxDelay time.Duration, attempt int) time.Duration {
	exp := math.Pow(2, float64(attempt-1))
	delay := time.Duration(float64(base) * exp)
	if delay > maxDelay {
		delay = maxDelay
	}
	jitter := 0.5 + cryptoFloat64()
	return time.Duration(float64(delay) * jitter)
}
