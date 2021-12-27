package app

import (
	"context"
	"errors"
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

	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/storagenode/executor"
	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/log"
	"github.com/kakao/varlog/pkg/util/telemetry"
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

	mp, stop, err := initMeterProvider(c, snid)
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

	// TODO: add initTimeout option
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sn, err := storagenode.New(ctx,
		storagenode.WithClusterID(cid),
		storagenode.WithStorageNodeID(snid),
		storagenode.WithListenAddress(c.String(flagListenAddress.Name)),
		storagenode.WithAdvertiseAddress(c.String(flagAdvertiseAddress.Name)),
		storagenode.WithVolumes(c.StringSlice(flagVolumes.Name)...),
		storagenode.WithExecutorOptions(
			executor.WithWriteQueueSize(c.Int(flagWriteQueueSize.Name)),
			executor.WithWriteBatchSize(c.Int(flagWriteBatchSize.Name)),
			executor.WithCommitQueueSize(c.Int(flagCommitQueueSize.Name)),
			executor.WithCommitBatchSize(c.Int(flagCommitBatchSize.Name)),
			executor.WithReplicateQueueSize(c.Int(flagReplicateQueueSize.Name)),
		),
		storagenode.WithStorageOptions(storageOpts...),
		storagenode.WithLogger(logger),
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

func initMeterProvider(c *cli.Context, snid types.StorageNodeID) (metric.MeterProvider, telemetry.StopMeterProvider, error) {
	var (
		err               error
		exporter          metricsdk.Exporter
		shutdown          telemetry.ShutdownExporter
		meterProviderOpts = []telemetry.MeterProviderOption{
			telemetry.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("sn"),
				semconv.ServiceNamespaceKey.String("varlog"),
				semconv.ServiceInstanceIDKey.String(snid.String()),
			)),
			telemetry.WithHostInstrumentation(),
			telemetry.WithRuntimeInstrumentation(),
		}
	)
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
