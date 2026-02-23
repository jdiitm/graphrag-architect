package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/metrics"
)

func TestHandlerReturnsPrometheusFormat(t *testing.T) {
	m := metrics.New()
	m.RecordConsumerLag("test-topic", 0, 0)
	handler := m.Handler()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "ingestion_consumer_lag") {
		t.Error("expected ingestion_consumer_lag metric in output")
	}
}

func TestRecordConsumerLag(t *testing.T) {
	m := metrics.New()
	m.RecordConsumerLag("raw-documents", 0, 150)

	handler := m.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "150") {
		t.Error("expected lag value 150 in output")
	}
}

func TestRecordBatchDuration(t *testing.T) {
	m := metrics.New()
	m.RecordBatchDuration(42.5)

	handler := m.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "ingestion_batch_duration") {
		t.Error("expected ingestion_batch_duration metric in output")
	}
}

func TestRecordDLQTotal(t *testing.T) {
	m := metrics.New()
	m.RecordDLQRouted()
	m.RecordDLQRouted()

	handler := m.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "ingestion_dlq_routed_total") {
		t.Error("expected ingestion_dlq_routed_total metric in output")
	}
}

func TestRecordJobsProcessedTotal(t *testing.T) {
	m := metrics.New()
	m.RecordJobProcessed("success")

	handler := m.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "ingestion_jobs_processed_total") {
		t.Error("expected ingestion_jobs_processed_total metric in output")
	}
}

func TestRecordDLQSinkError(t *testing.T) {
	m := metrics.New()
	m.RecordDLQSinkError()
	m.RecordDLQSinkError()
	m.RecordDLQSinkError()

	handler := m.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "ingestion_dlq_sink_error_total") {
		t.Error("expected ingestion_dlq_sink_error_total metric in output")
	}
	if !strings.Contains(body, "3") {
		t.Error("expected counter value 3 in output")
	}
}

func TestMetricsImplementsPipelineObserver(t *testing.T) {
	m := metrics.New()
	var obs metrics.PipelineObserver = m
	obs.RecordDLQSinkError()
}

func TestLastBatchTimeZeroWhenNoBatchRecorded(t *testing.T) {
	m := metrics.New()
	if !m.LastBatchTime().IsZero() {
		t.Fatal("expected zero time before any batch recorded")
	}
}

func TestLastBatchTimeUpdatedOnRecordBatchDuration(t *testing.T) {
	m := metrics.New()
	before := time.Now()
	m.RecordBatchDuration(1.0)
	after := time.Now()

	last := m.LastBatchTime()
	if last.Before(before) || last.After(after) {
		t.Fatalf("LastBatchTime() = %v, want between %v and %v", last, before, after)
	}
}

func TestLastBatchTimeUpdatesOnSubsequentBatches(t *testing.T) {
	m := metrics.New()
	m.RecordBatchDuration(1.0)
	first := m.LastBatchTime()

	time.Sleep(5 * time.Millisecond)
	m.RecordBatchDuration(2.0)
	second := m.LastBatchTime()

	if !second.After(first) {
		t.Fatalf("second LastBatchTime (%v) should be after first (%v)", second, first)
	}
}
