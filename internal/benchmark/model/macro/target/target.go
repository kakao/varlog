package target

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Target struct {
	ID   uint64
	Name string
}

func GetOrCreate(ctx context.Context, db *sql.DB, name string) (ret Target, err error) {
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
        INSERT INTO macrobenchmark_target (name)
        VALUES ($1) ON CONFLICT (name) DO NOTHING
    `)
	if err != nil {
		return fmt.Errorf("macrobenchmark_target: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	_, err = stmt.ExecContext(ctx, name)
	return err
}

func Get(ctx context.Context, db *sql.DB, name string) (ret Target, err error) {
	stmt, err := db.PrepareContext(ctx, `
        SELECT id, name
        FROM macrobenchmark_target
        WHERE name = $1
    `)
	if err != nil {
		return ret, fmt.Errorf("macrobenchmark_target: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	err = stmt.QueryRowContext(ctx, name).Scan(&ret.ID, &ret.Name)
	if err != nil {
		return ret, fmt.Errorf("macrobenchmark_target: %w", err)
	}
	return ret, nil
}
