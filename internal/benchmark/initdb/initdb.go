package initdb

import (
	"context"
	"database/sql"
	"errors"

	"github.com/kakao/varlog/internal/benchmark/model/executiontrigger"
	"github.com/kakao/varlog/internal/benchmark/model/macro/metric"
)

const (
	createTableExecutionTrigger = `
        create table if not exists execution_trigger (
            id   serial primary key,
            name varchar unique not null
        )
    `
	dropTableExecutionTrigger = "drop table if exists execution_trigger"

	createTableExecution = `
        create table if not exists execution (
            id          serial primary key,
            commit_hash varchar(40) unique not null,
            trigger_id  integer     not null references execution_trigger (id)
        )
    `
	dropTableExecution = "drop table if exists execution cascade"

	createTableMacrobenchmarkWorkload = `
        create table if not exists macrobenchmark_workload (
            id          serial primary key,
            name        varchar(128) unique not null,
            description text not null default ''
        )
    `
	dropTableMacrobenchmarkWorkload = "drop table if exists macrobenchmark_workload"
	insertMacrobenchmarkWorkload    = `
        INSERT INTO macrobenchmark_workload (name)
        VALUES ('one_logstream'),
               ('all_logstream')
    `

	createTableMacrobenchmark = `
		create table if not exists macrobenchmark (
			id           serial primary key,
			execution_id integer     not null references execution (id) on delete cascade,
			workload_id  integer     not null references macrobenchmark_workload (id),
			start_time   timestamptz not null,
			finish_time  timestamptz not null,
		    unique (execution_id, workload_id)
		)
    `
	dropTableMacrobenchmark = "drop table if exists macrobenchmark cascade"

	createTableMacrobenchmarkTarget = `
		create table if not exists macrobenchmark_target (
			id   serial primary key,
			name varchar(32) unique not null
		)
    `
	dropTableMacrobenchmarkTarget = "drop table if exists macrobenchmark_target"

	createTableMacrobenchmarkMetric = `
		create table if not exists macrobenchmark_metric (
			id          serial primary key,
			name        varchar(64) unique not null,
			description text not null default ''
		)
    `
	dropTableMacrobenchmarkMetric = "drop table if exists macrobenchmark_metric"

	createTableMacrobenchmarkResult = `
		create table if not exists macrobenchmark_result (
			macrobenchmark_id integer not null references macrobenchmark (id) on delete cascade,
			target_id         integer not null references macrobenchmark_target (id),
			metric_id         integer not null references macrobenchmark_metric (id),
			value             float   not null,
			primary key (macrobenchmark_id, target_id, metric_id)
		)
    `
	dropTableMacrobenchmarkResult = "drop table if exists macrobenchmark_result"
)

func CreateTables(ctx context.Context, db *sql.DB) error {
	return executeStatement(ctx, db,
		createTableExecutionTrigger,
		createTableExecution,
		createTableMacrobenchmarkWorkload,
		createTableMacrobenchmark,
		createTableMacrobenchmarkTarget,
		createTableMacrobenchmarkMetric,
		createTableMacrobenchmarkResult,
	)
}

func DropTables(ctx context.Context, db *sql.DB) error {
	return executeStatement(ctx, db,
		dropTableMacrobenchmarkResult,
		dropTableMacrobenchmark,
		dropTableExecution,
		dropTableMacrobenchmarkWorkload,
		dropTableExecutionTrigger,
		dropTableMacrobenchmarkTarget,
		dropTableMacrobenchmarkMetric,
	)
}

func InitTables(ctx context.Context, db *sql.DB) error {
	return errors.Join(
		executiontrigger.InitTable(ctx, db),
		metric.InitTable(ctx, db),
	)
}

func executeStatement(ctx context.Context, db *sql.DB, statements ...string) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	for _, stmt := range statements {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	return tx.Commit()
}
