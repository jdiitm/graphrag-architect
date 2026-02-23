package healthz

import (
	"encoding/json"
	"net/http"
	"time"
)

type ActivityReporter interface {
	LastBatchTime() time.Time
}

type Checker struct {
	reporter  ActivityReporter
	threshold time.Duration
}

type Option func(*Checker)

func WithThreshold(d time.Duration) Option {
	return func(c *Checker) {
		c.threshold = d
	}
}

func NewChecker(reporter ActivityReporter, opts ...Option) *Checker {
	c := &Checker{
		reporter:  reporter,
		threshold: 45 * time.Second,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

type response struct {
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	SinceLastPoll string `json:"since_last_poll,omitempty"`
}

func (c *Checker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	last := c.reporter.LastBatchTime()

	if last.IsZero() {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, response{Status: "unhealthy", Message: "no activity recorded"})
		return
	}

	elapsed := time.Since(last)
	if elapsed > c.threshold {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, response{
			Status:        "unhealthy",
			Message:       "stale: last poll exceeded threshold",
			SinceLastPoll: elapsed.Round(time.Millisecond).String(),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	writeJSON(w, response{
		Status:        "ok",
		SinceLastPoll: elapsed.Round(time.Millisecond).String(),
	})
}

func writeJSON(w http.ResponseWriter, v response) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
