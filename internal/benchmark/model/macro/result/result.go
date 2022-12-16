package result

import (
	"context"
	"database/sql"
	"fmt"
)

type ResultPoint struct {
	CommitHash string
	Value      float64
}

type Result struct {
	MacrobenchmarkID uint64
	TargetID         uint64
	MetricID         uint64
	Value            float64
}

func Create(ctx context.Context, db *sql.DB, result Result) error {
	stmt, err := db.PrepareContext(ctx, `
        INSERT INTO macrobenchmark_result (macrobenchmark_id, target_id, metric_id, value)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (macrobenchmark_id, target_id, metric_id) DO NOTHING
    `)
	if err != nil {
		return fmt.Errorf("macrobenchmark_result: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	_, err = stmt.ExecContext(ctx, result.MacrobenchmarkID, result.TargetID, result.MetricID, result.Value)
	if err != nil {
		return fmt.Errorf("macrobenchmark_result: %w", err)
	}
	return nil
}

func ListMacrobenchmarkResults(db *sql.DB, workload string, metric string, limit int) ([]ResultPoint, error) {
	stmt, err := db.Prepare(`
        SELECT e.commit_hash, mr.value
        FROM execution e
            JOIN macrobenchmark m on e.id = m.execution_id
            JOIN macrobenchmark_result mr on m.id = mr.macrobenchmark_id
            JOIN macrobenchmark_workload mw on m.workload_id = mw.id AND mw.name = $1
            JOIN macrobenchmark_metric mm on mr.metric_id = mm.id AND mm.name = $2 
        ORDER BY m.start_time
        LIMIT $3
    `)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = stmt.Close()
	}()

	rows, err := stmt.Query(workload, metric, limit)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var macrobenchmarks []ResultPoint
	for rows.Next() {
		m := ResultPoint{}
		if err := rows.Scan(&m.CommitHash, &m.Value); err != nil {
			return nil, err
		}
		macrobenchmarks = append(macrobenchmarks, m)
	}

	return macrobenchmarks, nil
}
