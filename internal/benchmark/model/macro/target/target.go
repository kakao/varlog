package target

import (
	"context"
	"database/sql"
	"fmt"
)

type Target struct {
	ID   uint64
	Name string
}

func GetOrCreate(ctx context.Context, db *sql.DB, name string) (Target, error) {
	stmt, err := db.Prepare(`
        INSERT INTO macrobenchmark_target (name)
        VALUES ($1) ON CONFLICT (name) DO NOTHING 
        RETURNING id, name
    `)
	if err != nil {
		return Target{}, fmt.Errorf("macrobenchmark_target: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	var ret Target
	row := stmt.QueryRowContext(ctx, name)
	err = row.Scan(&ret.ID, &ret.Name)
	if err != nil {
		return Target{}, fmt.Errorf("macrobenchmark_target: %w", err)
	}
	return ret, nil
}
