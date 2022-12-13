package macrobenchmark

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Macrobenchmark struct {
	ID          uint64
	ExecutionID uint64
	WorkloadID  uint64
	StartTime   time.Time
	FinishTime  time.Time
}

func GetOrCreate(ctx context.Context, db *sql.DB, m Macrobenchmark) (Macrobenchmark, error) {
	stmt, err := db.PrepareContext(ctx, `
        INSERT INTO macrobenchmark (execution_id, workload_id, start_time, finish_time)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (execution_id, workload_id) DO NOTHING
        RETURNING id, execution_id, workload_id, start_time, finish_time
    `)
	if err != nil {
		return Macrobenchmark{}, fmt.Errorf("macrobenchmark: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	var ret Macrobenchmark
	row := stmt.QueryRowContext(ctx, m.ExecutionID, m.WorkloadID, m.StartTime, m.FinishTime)
	err = row.Scan(&ret.ID, &ret.ExecutionID, &ret.WorkloadID, &ret.StartTime, &ret.FinishTime)
	if err != nil {
		return Macrobenchmark{}, fmt.Errorf("macrobenchmark: %w", err)
	}
	return ret, nil
}
