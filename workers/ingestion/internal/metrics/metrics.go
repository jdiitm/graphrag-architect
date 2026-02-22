package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry      *prometheus.Registry
	consumerLag   *prometheus.GaugeVec
	batchDuration prometheus.Histogram
	dlqRouted     prometheus.Counter
	dlqSinkError  prometheus.Counter
	jobsProcessed *prometheus.CounterVec
}

func New() *Metrics {
	reg := prometheus.NewRegistry()

	consumerLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ingestion_consumer_lag",
		Help: "Kafka consumer lag per topic and partition",
	}, []string{"topic", "partition"})

	batchDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingestion_batch_duration_seconds",
		Help:    "Duration of batch processing in seconds",
		Buckets: prometheus.DefBuckets,
	})

	dlqRouted := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingestion_dlq_routed_total",
		Help: "Total number of jobs routed to the DLQ",
	})

	dlqSinkError := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingestion_dlq_sink_error_total",
		Help: "Total number of DLQ sink publish failures",
	})

	jobsProcessed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingestion_jobs_processed_total",
		Help: "Total number of jobs processed by outcome",
	}, []string{"outcome"})

	reg.MustRegister(consumerLag, batchDuration, dlqRouted, dlqSinkError, jobsProcessed)

	return &Metrics{
		registry:      reg,
		consumerLag:   consumerLag,
		batchDuration: batchDuration,
		dlqRouted:     dlqRouted,
		dlqSinkError:  dlqSinkError,
		jobsProcessed: jobsProcessed,
	}
}

func (m *Metrics) RecordConsumerLag(topic string, partition int32, lag int64) {
	m.consumerLag.WithLabelValues(topic, strconv.Itoa(int(partition))).Set(float64(lag))
}

func (m *Metrics) RecordBatchDuration(seconds float64) {
	m.batchDuration.Observe(seconds)
}

func (m *Metrics) RecordDLQRouted() {
	m.dlqRouted.Inc()
}

func (m *Metrics) RecordDLQSinkError() {
	m.dlqSinkError.Inc()
}

func (m *Metrics) RecordJobProcessed(outcome string) {
	m.jobsProcessed.WithLabelValues(outcome).Inc()
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
