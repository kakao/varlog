package macrobenchmark

import (
	"context"
	"database/sql"
	"errors"
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

func GetOrCreate(ctx context.Context, db *sql.DB, m Macrobenchmark) (ret Macrobenchmark, err error) {
	ret, err = Get(ctx, db, m.ExecutionID, m.WorkloadID)
	if err == nil {
		return ret, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ret, err
	}
	if err := Create(ctx, db, m); err != nil {
		return Macrobenchmark{}, err
	}
	return Get(ctx, db, m.ExecutionID, m.WorkloadID)
}

func Create(ctx context.Context, db *sql.DB, m Macrobenchmark) error {
	stmt, err := db.PrepareContext(ctx, `
        INSERT INTO macrobenchmark (execution_id, workload_id, start_time, finish_time)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (execution_id, workload_id) DO NOTHING
    `)
	if err != nil {
		return fmt.Errorf("macrobenchmark: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	_, err = stmt.ExecContext(ctx, m.ExecutionID, m.WorkloadID, m.StartTime, m.FinishTime)
	return err
}

func Get(ctx context.Context, db *sql.DB, executionID, workloadID uint64) (ret Macrobenchmark, err error) {
	stmt, err := db.PrepareContext(ctx, `
        SELECT id, execution_id, workload_id, start_time, finish_time
        FROM macrobenchmark
        WHERE execution_id = $1 AND workload_id = $2
    `)
	if err != nil {
		return ret, fmt.Errorf("macrobenchmark: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	row := stmt.QueryRowContext(ctx, executionID, workloadID)
	err = row.Scan(&ret.ID, &ret.ExecutionID, &ret.WorkloadID, &ret.StartTime, &ret.FinishTime)
	if err != nil {
		return ret, fmt.Errorf("macrobenchmark: %w", err)
	}
	return ret, nil
}
