package workload

import (
	"context"
	"database/sql"
	"fmt"
)

type Workload struct {
	ID          uint64
	Name        string
	Description string
}

func GetOrCreate(ctx context.Context, db *sql.DB, name string) (Workload, error) {
	stmt, err := db.Prepare(`
        INSERT INTO macrobenchmark_workload (name)
        VALUES ($1) ON CONFLICT (name) DO NOTHING 
        RETURNING id, name, description
    `)
	if err != nil {
		return Workload{}, fmt.Errorf("macrobenchmark_workload: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	var ret Workload
	err = stmt.QueryRowContext(ctx, name).Scan(&ret.ID, &ret.Name, &ret.Description)
	if err != nil {
		return Workload{}, fmt.Errorf("macrobenchmark_workload: %w", err)
	}
	return ret, nil
}
