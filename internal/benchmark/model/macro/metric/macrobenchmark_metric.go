package metric

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	AppendRequestsPerSecond = "append_requests_per_second"
	AppendBytesPerSecond    = "append_bytes_per_second"
	AppendDurationMillis    = "append_durations_ms"
	SubscribeLogsPerSecond  = "subscribe_logs_per_second"
	SubscribeBytesPerSecond = "subscribe_bytes_per_second"
	EndToEndLatencyMillis   = "end_to_end_latency_ms"
)

type Metric struct {
	ID          uint64
	Name        string
	Description string
}

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

func List(ctx context.Context, db *sql.DB) ([]Metric, error) {
	stmt, err := db.PrepareContext(ctx, "SELECT id, name, description FROM macrobenchmark_metric")
	if err != nil {
		return nil, fmt.Errorf("macrobenchmark_metric: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var ret []Metric
	for rows.Next() {
		var cur Metric
		err = rows.Scan(&cur.ID, &cur.Name, &cur.Description)
		if err != nil {
			return nil, err
		}
		ret = append(ret, cur)
	}
	return ret, nil
}

func Get(ctx context.Context, db *sql.DB, name string) (Metric, error) {
	stmt, err := db.Prepare(`
        SELECT id, name, description 
        FROM macrobenchmark_metric 
        WHERE name = $1
    `)
	if err != nil {
		return Metric{}, fmt.Errorf("macrobenchmark_metric: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	var ret Metric
	err = stmt.QueryRowContext(ctx, name).Scan(&ret.ID, &ret.Name, &ret.Description)
	if err != nil {
		return Metric{}, fmt.Errorf("macrobenchmark_metric: %w", err)
	}
	return ret, nil
}
