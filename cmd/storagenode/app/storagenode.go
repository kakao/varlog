package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	metricsdk "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/executor"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/log"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/util/units"
)

func Main(c *cli.Context) error {
	cid, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return err
	}
	snid, err := types.ParseStorageNodeID(c.String(flagStorageNodeID.Name))
	if err != nil {
		return err
	}

	ballastSize, err := units.FromByteSizeString(c.String(flagBallastSize.Name))
	if err != nil {
		return err
	}

	// ballast
	ballast := make([]byte, ballastSize)
	_ = ballast

	// TODO: add initTimeout option
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mp, stop, err := initMeterProvider(ctx, c, snid)
	if err != nil {
		return err
	}
	telemetry.SetGlobalMeterProvider(mp)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.Duration(flagExporterStopTimeout.Name))
		defer cancel()
		stop(ctx)
	}()

	logOpts := []log.Option{
		log.WithHumanFriendly(),
		log.WithZapLoggerOptions(zap.AddStacktrace(zap.DPanicLevel)),
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
	defer logger.Sync()

	size, err := units.FromByteSizeString(c.String(flagServerReadBufferSize.Name))
	if err != nil {
		return fmt.Errorf("serverReadBufferSize: %w", err)
	}
	serverReadBufferSize := int(size)

	size, err = units.FromByteSizeString(c.String(flagServerWriteBufferSize.Name))
	if err != nil {
		return fmt.Errorf("serverWriteBufferSize: %w", err)
	}
	serverWriteBufferSize := int(size)

	size, err = units.FromByteSizeString(c.String(flagReplicationClientReadBufferSize.Name))
	if err != nil {
		return fmt.Errorf("replicationClientReadBufferSize: %w", err)
	}
	replicationClientReadBufferSize := int(size)

	size, err = units.FromByteSizeString(c.String(flagReplicationClientWriteBufferSize.Name))
	if err != nil {
		return fmt.Errorf("replicationClientWriteBufferSize: %w", err)
	}
	replicationClientWriteBufferSize := int(size)

	storageOpts := []storage.Option{}
	if c.Bool(flagDisableWriteSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutWriteSync())
	}
	if c.Bool(flagDisableCommitSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutCommitSync())
	}
	if c.Bool(flagDisableDeleteCommittedSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutDeleteCommittedSync())
	}
	if c.Bool(flagDisableDeleteUncommittedSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutDeleteUncommittedSync())
	}
	if c.IsSet(flagMemTableSizeBytes.Name) {
		storageOpts = append(storageOpts, storage.WithMemTableSizeBytes(c.Int(flagMemTableSizeBytes.Name)))
	}
	if c.IsSet(flagMemTableStopWritesThreshold.Name) {
		storageOpts = append(storageOpts, storage.WithMemTableStopWritesThreshold(c.Int(flagMemTableStopWritesThreshold.Name)))
	}
	if c.Bool(flagStorageDebugLog.Name) {
		storageOpts = append(storageOpts, storage.WithDebugLog())
	}

	sn, err := storagenode_deprecated.New(ctx,
		storagenode_deprecated.WithClusterID(cid),
		storagenode_deprecated.WithStorageNodeID(snid),
		storagenode_deprecated.WithListenAddress(c.String(flagListenAddress.Name)),
		storagenode_deprecated.WithAdvertiseAddress(c.String(flagAdvertiseAddress.Name)),
		storagenode_deprecated.WithVolumes(c.StringSlice(flagVolumes.Name)...),
		storagenode_deprecated.WithServerReadBufferSize(serverReadBufferSize),
		storagenode_deprecated.WithServerWriteBufferSize(serverWriteBufferSize),
		storagenode_deprecated.WithExecutorOptions(
			executor.WithWriteQueueSize(c.Int(flagWriteQueueSize.Name)),
			executor.WithWriteBatchSize(c.Int(flagWriteBatchSize.Name)),
			executor.WithCommitQueueSize(c.Int(flagCommitQueueSize.Name)),
			executor.WithCommitBatchSize(c.Int(flagCommitBatchSize.Name)),
			executor.WithReplicateQueueSize(c.Int(flagReplicateQueueSize.Name)),
			executor.WithReplicationClientReadBufferSize(replicationClientReadBufferSize),
			executor.WithReplicationClientWriteBufferSize(replicationClientWriteBufferSize),
		),
		storagenode_deprecated.WithStorageOptions(storageOpts...),
		storagenode_deprecated.WithLogger(logger),
	)
	if err != nil {
		return err
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigC
		sn.Close()
	}()

	return sn.Run()
}

func initMeterProvider(ctx context.Context, c *cli.Context, snid types.StorageNodeID) (metric.MeterProvider, telemetry.StopMeterProvider, error) {
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
		telemetry.WithHostInstrumentation(),
		telemetry.WithRuntimeInstrumentation(),
	}
	switch strings.ToLower(c.String(flagExporterType.Name)) {
	case "stdout":
		opts := []stdoutmetric.Option{}
		if c.Bool(flagStdoutExporterPrettyPrint.Name) {
			opts = append(opts, stdoutmetric.WithPrettyPrint())
		}
		exporter, shutdown, err = telemetry.NewStdoutExporter(opts...)
	case "otlp":
		opts := []otlpmetricgrpc.Option{}
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
