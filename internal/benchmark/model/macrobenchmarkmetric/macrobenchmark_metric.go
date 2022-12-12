package macrobenchmarkmetric

import (
	"context"
	"database/sql"
)

const (
	AppendRequestsPerSecond = "append_requests_per_second"
	AppendBytesPerSecond    = "append_bytes_per_second"
	AppendDurationMillis    = "append_durations_ms"
	SubscribeLogsPerSecond  = "subscribe_logs_per_second"
	SubscribeBytesPerSecond = "subscribe_bytes_per_second"
	EndToEndLatencyMillis   = "end_to_end_latency_ms"
)

func InitTable(ctx context.Context, db *sql.DB) error {
	stmt, err := db.PrepareContext(ctx, `
        INSERT INTO macrobenchmark_metric (name) 
        VALUES ($1), ($2), ($3), ($4), ($5), ($6)
        ON CONFLICT (name) DO NOTHING
    `)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close()
	}()
	_, err = stmt.ExecContext(ctx,
		AppendRequestsPerSecond,
		AppendBytesPerSecond,
		AppendDurationMillis,
		SubscribeLogsPerSecond,
		SubscribeBytesPerSecond,
		EndToEndLatencyMillis,
	)
	if err != nil {
		return err
	}
	return nil
}
