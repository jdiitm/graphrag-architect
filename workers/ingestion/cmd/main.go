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

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/blobstore"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/consumer"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dispatcher"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dlq"
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/healthz"
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
	healthChecker := healthz.NewChecker(m)
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.Handle("/healthz", healthChecker)
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("metrics server listening on %s", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()

	processorMode := envOrDefault("PROCESSOR_MODE", "kafka")

	var fp processor.DocumentProcessor
	if processorMode == "kafka" {
		parsedTopic := envOrDefault("KAFKA_PARSED_TOPIC", "graphrag.parsed")
		producer, prodErr := NewKafkaProducerClient(kafkaBrokers, parsedTopic)
		if prodErr != nil {
			log.Fatalf("create kafka producer for parsed topic: %v", prodErr)
		}
		defer producer.Close()
		blobBucket := envOrDefault("BLOB_BUCKET", "graphrag-ingestion")
		blobThreshold := envIntOrDefault("BLOB_THRESHOLD", processor.DefaultBlobThreshold)
		store := blobstore.NewInMemoryBlobStore()
		fp = processor.NewBlobForwardingProcessor(producer, store, parsedTopic, blobBucket, blobThreshold)
		log.Printf("processor mode: kafka with blob offload (topic=%s, bucket=%s, threshold=%d)",
			parsedTopic, blobBucket, blobThreshold)
	} else {
		var fpOpts []processor.ForwardingOption
		if authToken := os.Getenv("AUTH_TOKEN"); authToken != "" {
			fpOpts = append(fpOpts, processor.WithAuthToken(authToken))
			log.Println("auth token configured for orchestrator requests")
		}
		fp = processor.NewForwardingProcessor(orchestratorURL, &http.Client{Timeout: 30 * time.Second}, fpOpts...)
		log.Println("processor mode: http")
	}

	cfg := dispatcher.Config{
		NumWorkers: numWorkers,
		MaxRetries: maxRetries,
		JobBuffer:  numWorkers * 2,
		DLQBuffer:  numWorkers,
	}
	disp := dispatcher.New(cfg, fp, dispatcher.WithObserver(m))

	kafkaSource := NewKafkaJobSource(kafkaBrokers, kafkaTopic, consumerGroup)
	defer kafkaSource.Close()

	ackTimeoutSec := envIntOrDefault("ACK_TIMEOUT_SECONDS", 30)
	log.Printf("consumer ack timeout: %ds", ackTimeoutSec)
	cons := consumer.New(kafkaSource, disp.Jobs(), disp.Acks(),
		consumer.WithObserver(m),
		consumer.WithAckTimeout(time.Duration(ackTimeoutSec)*time.Second),
	)

	var sink dlq.DeadLetterSink
	dlqSinkMode := envOrDefault("DLQ_SINK", "kafka")
	if dlqSinkMode == "log" {
		sink = &LogDLQSink{}
		log.Println("DLQ sink: log-only (development mode)")
	} else {
		dlqTopic := envOrDefault("DLQ_TOPIC", "raw-documents.dlq")
		kafkaSink, err := NewKafkaDLQSink(kafkaBrokers, dlqTopic)
		if err != nil {
			log.Printf("FATAL: create kafka dlq sink: %v", err)
			return
		}
		defer kafkaSink.Close()
		sink = kafkaSink
		log.Printf("DLQ sink: kafka topic=%s", dlqTopic)
	}
	var dlqOpts []dlq.Option
	dlqOpts = append(dlqOpts, dlq.WithObserver(m))
	if fallbackPath := os.Getenv("DLQ_FALLBACK_PATH"); fallbackPath != "" {
		fileSink, err := dlq.NewFileSink(fallbackPath)
		if err != nil {
			log.Printf("FATAL: create dlq file fallback: %v", err)
			return
		}
		dlqOpts = append(dlqOpts, dlq.WithFallback(fileSink))
		log.Printf("DLQ fallback: file=%s", fallbackPath)
	}
	dlqHandler := dlq.NewHandler(disp.DLQ(), sink, dlqOpts...)

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
