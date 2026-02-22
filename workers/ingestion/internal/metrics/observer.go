package metrics

type PipelineObserver interface {
	RecordJobProcessed(outcome string)
	RecordDLQRouted()
	RecordDLQSinkError()
	RecordBatchDuration(seconds float64)
	RecordConsumerLag(topic string, partition int32, lag int64)
}

type NoopObserver struct{}

func (NoopObserver) RecordJobProcessed(_ string)                  {}
func (NoopObserver) RecordDLQRouted()                             {}
func (NoopObserver) RecordDLQSinkError()                          {}
func (NoopObserver) RecordBatchDuration(_ float64)                {}
func (NoopObserver) RecordConsumerLag(_ string, _ int32, _ int64) {}
