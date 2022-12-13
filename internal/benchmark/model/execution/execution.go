package execution

import (
	"context"
	"database/sql"
	"fmt"
)

type Execution struct {
	ID         uint64
	CommitHash string
	TriggerID  uint64
}

func GetOrCreate(ctx context.Context, db *sql.DB, newExec Execution) (Execution, error) {
	stmt, err := db.PrepareContext(ctx, `
        INSERT INTO execution (commit_hash, trigger_id)
        VALUES ($1, $2)
        ON CONFLICT (commit_hash) DO NOTHING
        RETURNING id, commit_hash, trigger_id
    `)
	if err != nil {
		return Execution{}, fmt.Errorf("execution: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	var ret Execution
	row := stmt.QueryRowContext(ctx, newExec.CommitHash, newExec.TriggerID)
	err = row.Scan(&ret.ID, &ret.CommitHash, &ret.TriggerID)
	if err != nil {
		return ret, fmt.Errorf("execution: %w", err)
	}
	return ret, err
}
