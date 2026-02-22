package dispatcher

import (
	"context"
	"sync"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/metrics"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/telemetry"
)

type Config struct {
	NumWorkers int
	MaxRetries int
	JobBuffer  int
	DLQBuffer  int
}

type Dispatcher struct {
	cfg       Config
	processor processor.DocumentProcessor
	observer  metrics.PipelineObserver
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
			jobCtx := telemetry.ExtractTraceContext(ctx, job.Headers)
			processCtx, processSpan := telemetry.StartProcessSpan(jobCtx, job)
			result := d.processWithRetry(processCtx, job)
			if result.Err != nil {
				_, dlqSpan := telemetry.StartDLQSpan(processCtx, result)
				result.Done = make(chan struct{})
				d.dlq <- result
				select {
				case <-result.Done:
				case <-ctx.Done():
					dlqSpan.End()
					processSpan.End()
					return
				}
				dlqSpan.End()
				d.observer.RecordDLQRouted()
				d.observer.RecordJobProcessed("dlq")
			} else {
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
	}
	return domain.Result{Job: job, Err: lastErr, Attempts: d.cfg.MaxRetries}
}
