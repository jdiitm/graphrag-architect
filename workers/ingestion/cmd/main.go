package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/consumer"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dispatcher"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dlq"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/metrics"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/processor"
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func main() {
	orchestratorURL := envOrDefault("ORCHESTRATOR_URL", "http://localhost:8000")
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := envOrDefault("KAFKA_TOPIC", "raw-documents")
	consumerGroup := envOrDefault("KAFKA_CONSUMER_GROUP", "ingestion-workers")
	numWorkers := envIntOrDefault("NUM_WORKERS", 4)
	maxRetries := envIntOrDefault("MAX_RETRIES", 3)

	metricsAddr := envOrDefault("METRICS_ADDR", ":9090")

	log.Printf("starting ingestion worker: brokers=%s topic=%s group=%s workers=%d",
		kafkaBrokers, kafkaTopic, consumerGroup, numWorkers)

	m := metrics.New()
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: mux}
	go func() {
		log.Printf("metrics server listening on %s", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()

	var fpOpts []processor.ForwardingOption
	if authToken := os.Getenv("AUTH_TOKEN"); authToken != "" {
		fpOpts = append(fpOpts, processor.WithAuthToken(authToken))
		log.Println("auth token configured for orchestrator requests")
	}
	fp := processor.NewForwardingProcessor(orchestratorURL, &http.Client{Timeout: 30 * time.Second}, fpOpts...)

	cfg := dispatcher.Config{
		NumWorkers: numWorkers,
		MaxRetries: maxRetries,
		JobBuffer:  numWorkers * 2,
		DLQBuffer:  numWorkers,
	}
	disp := dispatcher.New(cfg, fp, dispatcher.WithObserver(m))

	kafkaSource := NewKafkaJobSource(kafkaBrokers, kafkaTopic, consumerGroup)
	defer kafkaSource.Close()

	cons := consumer.New(kafkaSource, disp.Jobs(), disp.Acks(), consumer.WithObserver(m))

	var sink dlq.DeadLetterSink
	dlqSinkMode := envOrDefault("DLQ_SINK", "kafka")
	if dlqSinkMode == "log" {
		sink = &LogDLQSink{}
		log.Println("DLQ sink: log-only (development mode)")
	} else {
		dlqTopic := envOrDefault("DLQ_TOPIC", "raw-documents.dlq")
		kafkaSink, err := NewKafkaDLQSink(kafkaBrokers, dlqTopic)
		if err != nil {
			log.Fatalf("create kafka dlq sink: %v", err)
		}
		defer kafkaSink.Close()
		sink = kafkaSink
		log.Printf("DLQ sink: kafka topic=%s", dlqTopic)
	}
	dlqHandler := dlq.NewHandler(disp.DLQ(), sink, dlq.WithObserver(m))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		disp.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		dlqHandler.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := cons.Run(ctx); err != nil {
			log.Printf("consumer stopped: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	cancel()
	_ = metricsSrv.Close()
	wg.Wait()
	log.Println("shutdown complete")
}
