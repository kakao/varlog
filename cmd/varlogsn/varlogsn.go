package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	metricsdk "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/log"
	"github.com/kakao/varlog/pkg/util/telemetry"
	"github.com/kakao/varlog/pkg/util/units"
)

func main() {
	os.Exit(run())
}

func run() int {
	app := newStorageNodeApp()
	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "varlogsn: %+v\n", err)
		return -1
	}
	return 0
}

func start(c *cli.Context) error {
	level, err := zapcore.ParseLevel(c.String(flagLogLevel.Name))
	if err != nil {
		return err
	}

	logOpts := []log.Option{
		log.WithHumanFriendly(),
		log.WithZapLoggerOptions(zap.AddStacktrace(zap.DPanicLevel)),
		log.WithLogLevel(level),
	}
	if c.Bool(flagLogFileCompression.Name) {
		logOpts = append(logOpts, log.WithCompression())
	}
	if retention := c.Int(flagLogFileRetentionDays.Name); retention > 0 {
		logOpts = append(logOpts, log.WithAgeDays(retention))
	}
	if logDir := c.String(flagLogDir.Name); len(logDir) != 0 {
		absDir, err := filepath.Abs(logDir)
		if err != nil {
			return err
		}
		logOpts = append(logOpts, log.WithPath(filepath.Join(absDir, "storagenode.log")))
	}
	logger, err := log.New(logOpts...)
	if err != nil {
		return err
	}
	defer func() {
		_ = logger.Sync()
	}()

	clusterID, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return err
	}

	storageNodeID, err := types.ParseStorageNodeID(c.String(flagStorageNodeID.Name))
	if err != nil {
		return err
	}

	ballastSize, err := units.FromByteSizeString(c.String(flagBallastSize.Name))
	if err != nil {
		return err
	}

	readBufferSize, err := units.FromByteSizeString(c.String(flagServerReadBufferSize.Name))
	if err != nil {
		return fmt.Errorf("serverReadBufferSize: %w", err)
	}

	writeBufferSize, err := units.FromByteSizeString(c.String(flagServerWriteBufferSize.Name))
	if err != nil {
		return fmt.Errorf("serverWriteBufferSize: %w", err)
	}

	maxRecvMsgSize, err := units.FromByteSizeString(c.String(flagServerMaxRecvMsgSize.Name))
	if err != nil {
		return fmt.Errorf("serverMaxRecvMsgSize: %w", err)
	}

	replicateClientReadBufferSize, err := units.FromByteSizeString(c.String(flagReplicationClientReadBufferSize.Name))
	if err != nil {
		return fmt.Errorf("replicationClientReadBufferSize: %w", err)
	}

	replicateClientWriteBufferSize, err := units.FromByteSizeString(c.String(flagReplicationClientWriteBufferSize.Name))
	if err != nil {
		return fmt.Errorf("flagReplicationClientWriteBufferSize: %w", err)
	}

	logger = logger.Named("sn").With(zap.Uint32("cid", uint32(clusterID)), zap.Int32("snid", int32(storageNodeID)))

	mp, stop, err := initTelemetry(context.Background(), c, storageNodeID)
	if err != nil {
		return err
	}
	telemetry.SetGlobalMeterProvider(mp)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.Duration(flagExporterStopTimeout.Name))
		defer cancel()
		stop(ctx)
	}()

	lbaseMaxBytes, err := units.FromByteSizeString(c.String(flagStorageLBaseMaxBytes.Name))
	if err != nil {
		return err
	}
	storageOpts := []storage.Option{
		storage.WithL0CompactionThreshold(c.Int(flagStorageL0CompactionThreshold.Name)),
		storage.WithL0StopWritesThreshold(c.Int(flagStorageL0StopWritesThreshold.Name)),
		storage.WithLBaseMaxBytes(lbaseMaxBytes),
		storage.WithMaxOpenFiles(c.Int(flagStorageMaxOpenFiles.Name)),
		storage.WithMemTableSize(c.Int(flagStorageMemTableSize.Name)),
		storage.WithMemTableStopWritesThreshold(c.Int(flagStorageMemTableStopWritesThreshold.Name)),
		storage.WithMaxConcurrentCompaction(c.Int(flagStorageMaxConcurrentCompaction.Name)),
	}
	if c.Bool(flagStorageDisableWAL.Name) {
		storageOpts = append(storageOpts, storage.WithoutWAL())
	}
	if c.Bool(flagStorageNoSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutSync())
	}
	if c.Bool(flagStorageVerbose.Name) {
		storageOpts = append(storageOpts, storage.WithVerboseLogging())
	}

	sn, err := storagenode.NewStorageNode(
		storagenode.WithClusterID(clusterID),
		storagenode.WithStorageNodeID(storageNodeID),
		storagenode.WithListenAddress(c.String(flagListen.Name)),
		storagenode.WithAdvertiseAddress(c.String(flagAdvertise.Name)),
		storagenode.WithBallastSize(ballastSize),
		storagenode.WithVolumes(c.StringSlice(flagVolumes.Name)...),
		storagenode.WithGRPCServerReadBufferSize(readBufferSize),
		storagenode.WithGRPCServerWriteBufferSize(writeBufferSize),
		storagenode.WithGRPCServerMaxRecvMsgSize(maxRecvMsgSize),
		storagenode.WithReplicateClientReadBufferSize(replicateClientReadBufferSize),
		storagenode.WithReplicateClientWriteBufferSize(replicateClientWriteBufferSize),
		storagenode.WithDefaultLogStreamExecutorOptions(
			logstream.WithSequenceQueueCapacity(c.Int(flagLogStreamExecutorSequenceQueueCapacity.Name)),
			logstream.WithWriteQueueCapacity(c.Int(flagLogStreamExecutorWriteQueueCapacity.Name)),
			logstream.WithCommitQueueCapacity(c.Int(flagLogStreamExecutorCommitQueueCapacity.Name)),
			logstream.WithReplicateClientQueueCapacity(c.Int(flagLogStreamExecutorReplicateclientQueueCapacity.Name)),
		),
		storagenode.WithDefaultStorageOptions(storageOpts...),
		storagenode.WithLogger(logger),
	)
	if err != nil {
		return err
	}

	var g errgroup.Group
	quit := make(chan struct{})
	g.Go(func() error {
		defer close(quit)
		return sn.Serve()
	})
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	g.Go(func() error {
		select {
		case sig := <-sigC:
			return multierr.Append(fmt.Errorf("caught signal %s", sig), sn.Close())
		case <-quit:
			return nil
		}
	})
	return g.Wait()
}

func initTelemetry(ctx context.Context, c *cli.Context, snid types.StorageNodeID) (metric.MeterProvider, telemetry.StopMeterProvider, error) {
	var (
		err      error
		exporter metricsdk.Exporter
		shutdown telemetry.ShutdownExporter
	)

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("sn"),
			semconv.ServiceNamespaceKey.String("varlog"),
			semconv.ServiceInstanceIDKey.String(snid.String()),
		))
	if err != nil {
		return nil, nil, err
	}

	meterProviderOpts := []telemetry.MeterProviderOption{
		telemetry.WithResource(res),
		telemetry.WithRuntimeInstrumentation(),
		telemetry.WithAggregatorSelector(simple.NewWithInexpensiveDistribution()),
	}
	switch strings.ToLower(c.String(flagExporterType.Name)) {
	case "stdout":
		var opts []stdoutmetric.Option
		if c.Bool(flagStdoutExporterPrettyPrint.Name) {
			opts = append(opts, stdoutmetric.WithPrettyPrint())
		}
		exporter, shutdown, err = telemetry.NewStdoutExporter(opts...)
	case "otlp":
		var opts []otlpmetricgrpc.Option
		if c.Bool(flagOTLPExporterInsecure.Name) {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		if !c.IsSet(flagOTLPExporterEndpoint.Name) {
			return nil, nil, errors.New("no exporter endpoint")
		}
		opts = append(opts, otlpmetricgrpc.WithEndpoint(c.String(flagOTLPExporterEndpoint.Name)))
		exporter, shutdown, err = telemetry.NewOLTPExporter(context.Background(), opts...)
	}
	if err != nil {
		return nil, nil, err
	}

	if exporter != nil {
		meterProviderOpts = append(meterProviderOpts, telemetry.WithExporter(exporter, shutdown))
	}

	return telemetry.NewMeterProvider(meterProviderOpts...)
}
