package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/benchmark"
	"github.com/kakao/varlog/internal/benchmark/database"
	"github.com/kakao/varlog/internal/benchmark/model/execution"
	"github.com/kakao/varlog/internal/benchmark/model/executiontrigger"
	"github.com/kakao/varlog/internal/benchmark/model/macro/macrobenchmark"
	"github.com/kakao/varlog/internal/benchmark/model/macro/metric"
	"github.com/kakao/varlog/internal/benchmark/model/macro/result"
	"github.com/kakao/varlog/internal/benchmark/model/macro/target"
	"github.com/kakao/varlog/internal/benchmark/model/macro/workload"
)

var (
	flagCommitHash = &cli.StringFlag{
		Name:     "commit-hash",
		Usage:    "Commit Hash",
		Required: true,
	}

	flagWorkload = &cli.StringFlag{
		Name:     "workload",
		Required: true,
	}

	flagTrigger = &cli.StringFlag{
		Name:  "trigger",
		Value: executiontrigger.Command,
	}

	flagFile = &cli.PathFlag{
		Name: "file",
	}
)

func newCommandSave() *cli.Command {
	return &cli.Command{
		Name:  "save",
		Usage: "Save benchmark results",
		Flags: []cli.Flag{
			flagDatabaseHost,
			flagDatabasePort,
			flagDatabaseUser,
			flagDatabasePassword,
			flagDatabaseName,
			flagCommitHash,
			flagWorkload,
			flagTrigger,
			flagFile,
		},
		Action: runCommandSave,
	}
}

func runCommandSave(c *cli.Context) error {
	db, err := database.ConnectDatabase(
		c.String(flagDatabaseHost.Name),
		c.Int(flagDatabasePort.Name),
		c.String(flagDatabaseUser.Name),
		c.String(flagDatabasePassword.Name),
		c.String(flagDatabaseName.Name),
	)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	reader, err := newReportsReader(c.Path(flagFile.Name))
	if err != nil {
		return fmt.Errorf("read input %s: %w", c.Path(flagFile.Name), err)
	}
	defer func() {
		_ = reader.Close()
	}()
	var lastLine string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		lastLine = scanner.Text()
	}
	var trs benchmark.TargetReports
	if err := json.Unmarshal([]byte(lastLine), &trs); err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}
	return save(c, db, trs)
}

func save(c *cli.Context, db *sql.DB, trs benchmark.TargetReports) error {
	triggerName := c.String(flagTrigger.Name)
	trigger, err := executiontrigger.Get(context.TODO(), db, triggerName)
	if err != nil {
		return fmt.Errorf("find execution trigger %s: %w", triggerName, err)
	}

	commitHash := c.String(flagCommitHash.Name)
	exec, err := execution.GetOrCreate(context.TODO(), db, execution.Execution{
		CommitHash: commitHash,
		TriggerID:  trigger.ID,
	})
	if err != nil {
		return fmt.Errorf("find or create execution %s: %w", commitHash, err)
	}

	workloadName := c.String(flagWorkload.Name)
	wrk, err := workload.GetOrCreate(context.TODO(), db, workloadName)
	if err != nil {
		return fmt.Errorf("find or create workload %s: %w", workloadName, err)
	}

	mb, err := macrobenchmark.GetOrCreate(context.TODO(), db, macrobenchmark.Macrobenchmark{
		ExecutionID: exec.ID,
		WorkloadID:  wrk.ID,
		StartTime:   time.Now(),
		FinishTime:  time.Now(),
	})
	if err != nil {
		return fmt.Errorf("find or create macrobenchmark %s(%d), %s(%d): %w", commitHash, exec.ID, workloadName, wrk.ID, err)
	}

	metrics, err := metric.List(context.TODO(), db)
	if err != nil {
		return fmt.Errorf("list metrics: %w", err)
	}

	for _, rpt := range trs.Reports {
		tgt, err := target.GetOrCreate(context.TODO(), db, rpt.Target)
		if err != nil {
			return fmt.Errorf("find or create target %s: %w", rpt.Target, err)
		}
		for _, m := range metrics {
			var value float64
			switch m.Name {
			case metric.AppendRequestsPerSecond:
				value = rpt.Total.AppendReport.RequestsPerSecond
			case metric.AppendBytesPerSecond:
				value = rpt.Total.AppendReport.BytesPerSecond
			case metric.AppendDurationMillis:
				value = rpt.Total.AppendReport.Duration
			case metric.SubscribeLogsPerSecond:
				value = rpt.Total.SubscribeReport.LogsPerSecond
			case metric.SubscribeBytesPerSecond:
				value = rpt.Total.SubscribeReport.BytesPerSecond
			case metric.EndToEndLatencyMillis:
				value = rpt.Total.EndToEndReport.Latency
			default:
				return fmt.Errorf("unknown metric name %s", m.Name)
			}
			if err := result.Create(context.TODO(), db, result.Result{
				MacrobenchmarkID: mb.ID,
				TargetID:         tgt.ID,
				MetricID:         m.ID,
				Value:            value,
			}); err != nil {
				return fmt.Errorf("create result: %w", err)
			}
			slog.Info("inserted",
				slog.Uint64("executionID", exec.ID),
				slog.Uint64("macrobenchmarkID", mb.ID),
				slog.Uint64("workloadID", wrk.ID),
				slog.Uint64("targetID", tgt.ID),
				slog.Uint64("metricID", m.ID),
			)
		}
	}
	return nil
}

func newReportsReader(path string) (io.ReadCloser, error) {
	if len(path) > 0 {
		return os.Open(path)
	}
	return os.Stdin, nil
}
