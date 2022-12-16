package workload

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Workload struct {
	ID          uint64
	Name        string
	Description string
}

func GetOrCreate(ctx context.Context, db *sql.DB, name string) (ret Workload, err error) {
	ret, err = Get(ctx, db, name)
	if err == nil {
		return ret, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ret, err
	}
	if err := Create(ctx, db, name); err != nil {
		return ret, err
	}
	return Get(ctx, db, name)
}

func Create(ctx context.Context, db *sql.DB, name string) error {
	stmt, err := db.PrepareContext(ctx, `
        INSERT INTO macrobenchmark_workload (name)
        VALUES ($1) ON CONFLICT (name) DO NOTHING
    `)
	if err != nil {
		return fmt.Errorf("macrobenchmark_workload: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	_, err = stmt.ExecContext(ctx, name)
	return err
}

func Get(ctx context.Context, db *sql.DB, name string) (ret Workload, err error) {
	stmt, err := db.PrepareContext(ctx, `
        SELECT id, name
        FROM macrobenchmark_workload
        WHERE name = $1
    `)
	if err != nil {
		return ret, fmt.Errorf("macrobenchmark_workload: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	err = stmt.QueryRowContext(ctx, name).Scan(&ret.ID, &ret.Name)
	if err != nil {
		return ret, fmt.Errorf("macrobenchmark_workload: %w", err)
	}
	return ret, nil
}
