package domain

import "time"

type Job struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	Headers   map[string]string
	Timestamp time.Time
}

type Result struct {
	Job      Job
	Err      error
	Attempts int
}
