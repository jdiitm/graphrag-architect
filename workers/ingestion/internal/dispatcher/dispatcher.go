package dispatcher

import (
	"context"
	"sync"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
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
	jobs      chan domain.Job
	dlq       chan domain.Result
	wg        sync.WaitGroup
}

func New(cfg Config, proc processor.DocumentProcessor) *Dispatcher {
	return &Dispatcher{
		cfg:       cfg,
		processor: proc,
		jobs:      make(chan domain.Job, cfg.JobBuffer),
		dlq:       make(chan domain.Result, cfg.DLQBuffer),
	}
}

func (d *Dispatcher) Jobs() chan<- domain.Job {
	return d.jobs
}

func (d *Dispatcher) DLQ() <-chan domain.Result {
	return d.dlq
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
			result := d.processWithRetry(ctx, job)
			if result.Err != nil {
				d.dlq <- result
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
