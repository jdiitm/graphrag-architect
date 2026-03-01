package main

import (
	"context"
	"fmt"
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
	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/dedup"
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

func validateBlobStoreForProduction(deploymentMode, blobStoreType string) error {
	if deploymentMode == "production" && blobStoreType != "s3" {
		return fmt.Errorf(
			"BLOB_STORE_TYPE=%q is unsafe for DEPLOYMENT_MODE=production; "+
				"pod restarts will lose all >64KB documents; set BLOB_STORE_TYPE=s3",
			blobStoreType,
		)
	}
	return nil
}

func validateDedupStoreForProduction(deploymentMode, dedupStoreType string) error {
	if deploymentMode == "production" && dedupStoreType == "noop" {
		return fmt.Errorf(
			"DEDUP_STORE_TYPE=%q is unsafe for DEPLOYMENT_MODE=production; "+
				"deduplication is completely inactive; set DEDUP_STORE_TYPE to redis or memory",
			dedupStoreType,
		)
	}
	return nil
}

func validateInflightForProduction(deploymentMode string, maxInflight int) error {
	if deploymentMode == "production" && maxInflight <= 0 {
		return fmt.Errorf(
			"MAX_INFLIGHT=%d is unsafe for DEPLOYMENT_MODE=production; "+
				"unlimited in-flight messages disable backpressure; "+
				"set MAX_INFLIGHT to a positive integer (e.g. 100)",
			maxInflight,
		)
	}
	return nil
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
	deploymentMode := envOrDefault("DEPLOYMENT_MODE", "")
	dedupStoreType := envOrDefault("DEDUP_STORE_TYPE", "noop")
	if err := validateDedupStoreForProduction(deploymentMode, dedupStoreType); err != nil {
		log.Fatalf("production safety check failed: %v", err)
	}
	maxInflight := envIntOrDefault("MAX_INFLIGHT", 0)
	if err := validateInflightForProduction(deploymentMode, maxInflight); err != nil {
		log.Fatalf("production safety check failed: %v", err)
	}

	var fp processor.DocumentProcessor
	switch processorMode {
	case "kafka":
		parsedTopic := envOrDefault("KAFKA_PARSED_TOPIC", "graphrag.parsed")
		blobBucket := envOrDefault("BLOB_BUCKET", "graphrag-ingestion")
		blobThreshold := envIntOrDefault("BLOB_THRESHOLD", processor.DefaultBlobThreshold)
		blobStoreType := envOrDefault("BLOB_STORE_TYPE", "memory")
		if err := validateBlobStoreForProduction(deploymentMode, blobStoreType); err != nil {
			log.Fatalf("production safety check failed: %v", err)
		}
		blobRegion := envOrDefault("AWS_REGION", "us-east-1")
		store, blobErr := blobstore.NewBlobStoreFromEnv(blobStoreType, blobBucket, blobRegion)
		if blobErr != nil {
			log.Fatalf("create blob store: %v", blobErr)
		}
		producer, prodErr := NewKafkaProducerClient(kafkaBrokers, parsedTopic)
		if prodErr != nil {
			log.Fatalf("create kafka producer for parsed topic: %v", prodErr)
		}
		defer producer.Close()
		fp = processor.NewBlobForwardingProcessor(producer, store, parsedTopic, blobBucket, blobThreshold)
		log.Printf("processor mode: kafka with blob offload (topic=%s, bucket=%s, store=%s, threshold=%d)",
			parsedTopic, blobBucket, blobStoreType, blobThreshold)
	case "ast":
		astTopic := envOrDefault("KAFKA_AST_TOPIC", "graphrag.ast-parsed")
		producer, prodErr := NewKafkaProducerClient(kafkaBrokers, astTopic)
		if prodErr != nil {
			log.Printf("FATAL: create kafka producer for ast topic: %v", prodErr)
			return
		}
		defer producer.Close()
		fp = processor.NewASTForwardingProcessor(producer, astTopic)
		log.Printf("processor mode: ast extraction (topic=%s)", astTopic)
	default:
		var fpOpts []processor.ForwardingOption
		if authToken := os.Getenv("AUTH_TOKEN"); authToken != "" {
			fpOpts = append(fpOpts, processor.WithAuthToken(authToken))
			log.Println("auth token configured for orchestrator requests")
		}
		fp = processor.NewForwardingProcessor(orchestratorURL, &http.Client{Timeout: 30 * time.Second}, fpOpts...)
		log.Println("processor mode: http")
	}

	dedupRedisURL := os.Getenv("DEDUP_REDIS_URL")
	dedupCapacity := envIntOrDefault("DEDUP_LRU_CAPACITY", 10000)
	dedupTTLHours := envIntOrDefault("DEDUP_TTL_HOURS", 168)
	dedupStore := dedup.NewStoreFromEnv(dedupStoreType, dedupRedisURL, dedupCapacity, dedupTTLHours)

	cfg := dispatcher.Config{
		NumWorkers: numWorkers,
		MaxRetries: maxRetries,
		JobBuffer:  numWorkers * 2,
		DLQBuffer:  numWorkers,
	}
	disp := dispatcher.New(cfg, fp, dispatcher.WithObserver(m), dispatcher.WithDedup(dedupStore))

	kafkaSource := NewKafkaJobSource(kafkaBrokers, kafkaTopic, consumerGroup)
	defer kafkaSource.Close()

	ackTimeoutSec := envIntOrDefault("ACK_TIMEOUT_SECONDS", 30)
	log.Printf("consumer ack timeout: %ds, max inflight: %d", ackTimeoutSec, maxInflight)
	consumerOpts := []consumer.ConsumerOption{
		consumer.WithObserver(m),
		consumer.WithAckTimeout(time.Duration(ackTimeoutSec) * time.Second),
	}
	if maxInflight > 0 {
		consumerOpts = append(consumerOpts, consumer.WithMaxInflight(maxInflight))
	}
	cons := consumer.New(kafkaSource, disp.Jobs(), disp.Acks(), consumerOpts...)

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
