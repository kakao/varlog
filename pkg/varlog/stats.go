package varlog

import (
	"context"
	"time"
)

// SubscribeStats contains statistics for a single log entry processed by a
// subscription. It is passed to the SubscribeObserver for each log entry.
type SubscribeStats struct {
	// AggregationEnqueueDuration is the time it takes to enqueue a single log
	// entry into the internal aggregation buffer. A long duration may indicate
	// high load or lock contention within the client.
	AggregationEnqueueDuration time.Duration

	// AggregationBufferWait is the time a single log entry spends waiting in
	// the aggregation buffer. A high value can indicate that the internal
	// aggregator goroutine is not being scheduled frequently enough, possibly
	// due to high CPU load or scheduler latency.
	AggregationBufferWait time.Duration

	// DispatchQueueWait is the time a single log entry spends waiting in the
	// dispatch queue before being passed to the user's callback.
	DispatchQueueWait time.Duration

	// ProcessDuration is the time it takes for the user-provided callback to
	// execute for a single log entry.
	ProcessDuration time.Duration
}

// SubscribeObserver is an interface for observing subscription statistics.
type SubscribeObserver interface {
	// Observe is called for each log entry processed by the subscription,
	// allowing the user to collect and handle statistics.
	Observe(context.Context, SubscribeStats)
}
