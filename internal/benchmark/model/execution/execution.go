package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Execution struct {
	ID         uint64
	CommitHash string
	TriggerID  uint64
}

func GetOrCreate(ctx context.Context, db *sql.DB, newExec Execution) (ret Execution, err error) {
	ret, err = Get(ctx, db, newExec.CommitHash)
	if err == nil {
		return ret, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ret, err
	}
	if err := Create(ctx, db, newExec); err != nil {
		return Execution{}, err
	}
	return Get(ctx, db, newExec.CommitHash)
}

func Create(ctx context.Context, db *sql.DB, newExec Execution) error {
	stmt, err := db.PrepareContext(ctx, `
        INSERT INTO execution (commit_hash, trigger_id)
        VALUES ($1, $2)
        ON CONFLICT (commit_hash) DO NOTHING
    `)
	if err != nil {
		return fmt.Errorf("execution: insert %+v: %w", newExec, err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	_, err = stmt.ExecContext(ctx, newExec.CommitHash, newExec.TriggerID)
	if err != nil {
		return fmt.Errorf("execution: insert %+v: %w", newExec, err)
	}
	return nil
}

func Get(ctx context.Context, db *sql.DB, commitHash string) (ret Execution, err error) {
	stmt, err := db.PrepareContext(ctx, `
        SELECT id, commit_hash, trigger_id
        FROM execution
        WHERE commit_hash = $1
    `)
	if err != nil {
		return Execution{}, fmt.Errorf("execution: get %s: %w", commitHash, err)
	}
	defer func() {
		_ = stmt.Close()
	}()
	err = stmt.QueryRowContext(ctx, commitHash).Scan(&ret.ID, &ret.CommitHash, &ret.TriggerID)
	if err != nil {
		return Execution{}, fmt.Errorf("execution: get %s: %w", commitHash, err)
	}
	return ret, nil
}
