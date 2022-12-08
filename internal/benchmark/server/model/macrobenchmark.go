package model

import (
	"database/sql"
)

type MacrobenchmarkResult struct {
	CommitHash string
	Values     float64
}

func ListMacrobenchmarkResults(db *sql.DB, workload string, metric string, limit int) ([]MacrobenchmarkResult, error) {
	stmt, err := db.Prepare(`
        SELECT e.commit_hash, mr.value
        FROM execution e
            JOIN macrobenchmark m on e.id = m.execution_id
            JOIN macrobenchmark_result mr on m.id = mr.macrobenchmark_id
            JOIN macrobenchmark_workload mw on m.workload_id = mw.id AND mw.name = $1
            JOIN macrobenchmark_metric mm on mr.metric_id = mm.id AND mm.name = $2 
        ORDER BY e.start_time
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

	var macrobenchmarks []MacrobenchmarkResult
	for rows.Next() {
		m := MacrobenchmarkResult{}
		if err := rows.Scan(&m.CommitHash, &m.Values); err != nil {
			return nil, err
		}
		macrobenchmarks = append(macrobenchmarks, m)
	}

	return macrobenchmarks, nil
}
