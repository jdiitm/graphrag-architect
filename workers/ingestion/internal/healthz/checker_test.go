package healthz_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/healthz"
)

type stubReporter struct {
	lastBatch time.Time
}

func (s *stubReporter) LastBatchTime() time.Time {
	return s.lastBatch
}

func TestCheckerHealthyAfterRecentActivity(t *testing.T) {
	reporter := &stubReporter{lastBatch: time.Now()}
	checker := healthz.NewChecker(reporter, healthz.WithThreshold(45*time.Second))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	checker.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "ok") {
		t.Errorf("body = %q, want to contain 'ok'", rec.Body.String())
	}
}

func TestCheckerUnhealthyWhenNoActivity(t *testing.T) {
	reporter := &stubReporter{}
	checker := healthz.NewChecker(reporter, healthz.WithThreshold(45*time.Second))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	checker.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "no activity recorded") {
		t.Errorf("body = %q, want to contain 'no activity recorded'", rec.Body.String())
	}
}

func TestCheckerUnhealthyWhenActivityStale(t *testing.T) {
	reporter := &stubReporter{lastBatch: time.Now().Add(-2 * time.Minute)}
	checker := healthz.NewChecker(reporter, healthz.WithThreshold(45*time.Second))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	checker.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "stale") {
		t.Errorf("body = %q, want to contain 'stale'", rec.Body.String())
	}
}

func TestCheckerDefaultThresholdIs45Seconds(t *testing.T) {
	reporter := &stubReporter{lastBatch: time.Now().Add(-40 * time.Second)}
	checker := healthz.NewChecker(reporter)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	checker.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d (40s ago should be within default 45s threshold)", rec.Code, http.StatusOK)
	}
}

func TestCheckerCustomThreshold(t *testing.T) {
	reporter := &stubReporter{lastBatch: time.Now().Add(-10 * time.Second)}
	checker := healthz.NewChecker(reporter, healthz.WithThreshold(5*time.Second))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	checker.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d (10s ago exceeds 5s threshold)", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestCheckerResponseIsJSON(t *testing.T) {
	reporter := &stubReporter{lastBatch: time.Now()}
	checker := healthz.NewChecker(reporter)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	checker.ServeHTTP(rec, req)

	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}
