package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"syscall"

	"github.com/urfave/cli/v2"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/stats/opentelemetry"
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/log"
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
	logOpts, err := flags.ParseLoggerFlags(c, "varlogsn.log")
	if err != nil {
		return err
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

	storageNodeID := types.StorageNodeID(flagStorageNodeID.Get(c))

	ballastSize, err := units.FromByteSizeString(c.String(flagBallastSize.Name))
	if err != nil {
		return err
	}

	grpcServerOpts, err := flags.ParseGRPCServerOptionFlags(c)
	if err != nil {
		return err
	}
	grpcDialOpts, err := flags.ParseGRPCDialOptionFlags(c)
	if err != nil {
		return err
	}

	logger = logger.Named("sn").With(zap.Int32("cid", int32(clusterID)), zap.Int32("snid", int32(storageNodeID)))

	meterProviderOpts, err := flags.ParseTelemetryFlags(context.Background(), c, "sn", storageNodeID.String(), clusterID)
	if err != nil {
		return err
	}
	mp, stop, err := opentelemetry.NewMeterProvider(meterProviderOpts...)
	if err != nil {
		return err
	}
	opentelemetry.SetGlobalMeterProvider(mp)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.Duration(flags.TelemetryExporterStopTimeout.Name))
		defer cancel()
		_ = stop(ctx)
	}()

	cacheSize, err := units.FromByteSizeString(c.String(flagStorageCacheSize.Name))
	if err != nil {
		return err
	}
	storageCache := storage.NewCache(cacheSize)
	defer storageCache.Unref()

	storageOpts, err := parseStorageOptions(c, storage.WithCache(storageCache))
	if err != nil {
		return err
	}

	for _, flagStorageStore := range []*cli.GenericFlag{flagStorageValueStore, flagStorageCommitStore} {
		iface := c.Generic(flagStorageStore.Name)
		if iface == nil {
			continue
		}
		s, ok := iface.(*StorageStoreSetting)
		if !ok || s == nil {
			continue
		}

		storeOpts := []storage.StoreOption{
			storage.WithWAL(s.wal),
			storage.WithSyncWAL(s.syncWAL),
			storage.WithWALBytesPerSync(int(s.walBytesPerSync)),
			storage.WithSSTBytesperSync(int(s.sstBytesPerSync)),
			storage.WithMemTableSize(int(s.memTableSize)),
			storage.WithMemTableStopWritesThreshold(s.memTableStopWritesThreshold),
			storage.WithFlushSplitBytes(s.flushSplitBytes),
			storage.WithL0CompactionFileThreshold(s.l0CompactionFileThreshold),
			storage.WithL0CompactionThreshold(s.l0CompactionThreshold),
			storage.WithL0StopWritesThreshold(s.l0StopWritesThreshold),
			storage.WithL0TargetFileSize(s.l0TargetFileSize),
			storage.WithLBaseMaxBytes(s.lbaseMaxBytes),
			storage.WithMaxConcurrentCompaction(s.maxConcurrentCompactions),
			storage.WithTrimDelay(s.trimDelay),
			storage.WithTrimRateByte(int(s.trimRate)),
			storage.WithMaxOpenFiles(s.maxOpenFiles),
			storage.EnableTelemetry(s.enableTelemetry),
			storage.WithVerbose(s.verbose),
		}
		switch flagStorageStore.Name {
		case flagStorageValueStore.Name:
			storageOpts = append(storageOpts, storage.WithValueStoreOptions(storeOpts...))
		case flagStorageCommitStore.Name:
			storageOpts = append(storageOpts, storage.WithCommitStoreOptions(storeOpts...))
		}

	}

	snOpts := []storagenode.Option{
		storagenode.WithClusterID(clusterID),
		storagenode.WithStorageNodeID(storageNodeID),
		storagenode.WithListenAddress(c.String(flagListen.Name)),
		storagenode.WithAdvertiseAddress(c.String(flagAdvertise.Name)),
		storagenode.WithBallastSize(ballastSize),
		storagenode.WithVolumes(c.StringSlice(flagVolumes.Name)...),
		storagenode.WithDefaultLogStreamExecutorOptions(
			logstream.WithSequenceQueueCapacity(c.Int(flagLogStreamExecutorSequenceQueueCapacity.Name)),
			logstream.WithWriteQueueCapacity(c.Int(flagLogStreamExecutorWriteQueueCapacity.Name)),
			logstream.WithCommitQueueCapacity(c.Int(flagLogStreamExecutorCommitQueueCapacity.Name)),
			logstream.WithReplicateClientQueueCapacity(c.Int(flagLogStreamExecutorReplicateclientQueueCapacity.Name)),
		),
		storagenode.WithMaxLogStreamReplicasCount(int32(c.Int(flagMaxLogStreamReplicasCount.Name))),
		storagenode.WithAppendPipelineSize(int32(c.Int(flagAppendPipelineSize.Name))),
		storagenode.WithDefaultStorageOptions(storageOpts...),
		storagenode.WithDefaultGRPCServerOptions(grpcServerOpts...),
		storagenode.WithDefaultGRPCDialOptions(grpcDialOpts...),
		storagenode.WithLogger(logger),
	}

	sn, err := storagenode.NewStorageNode(snOpts...)
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

func parseStorageOptions(c *cli.Context, commonStoreOpts ...storage.StoreOption) (opts []storage.Option, err error) {
	l0CompactionFileThreshold, err := getStorageDBFlagValues(c.IntSlice(flagStorageL0CompactionFileThreshold.Name))
	if err != nil {
		return nil, err
	}
	l0CompactionThreshold, err := getStorageDBFlagValues(c.IntSlice(flagStorageL0CompactionThreshold.Name))
	if err != nil {
		return nil, err
	}
	l0StopWritesThreshold, err := getStorageDBFlagValues(c.IntSlice(flagStorageL0StopWritesThreshold.Name))
	if err != nil {
		return nil, err
	}
	l0TargetFileSizeStr, err := getStorageDBFlagValues(c.StringSlice(flagStorageL0TargetFileSize.Name))
	if err != nil {
		return nil, err
	}
	l0TargetFileSize, err := mapf(l0TargetFileSizeStr[:], func(s string) (int64, error) {
		return units.FromByteSizeString(s)
	})
	if err != nil {
		return nil, err
	}
	flushSplitBytesStr, err := getStorageDBFlagValues(c.StringSlice(flagStorageFlushSplitBytes.Name))
	if err != nil {
		return nil, err
	}
	flushSplitBytes, err := mapf(flushSplitBytesStr[:], func(s string) (int64, error) {
		return units.FromByteSizeString(s)
	})
	if err != nil {
		return nil, err
	}
	lbaseMaxBytesStr, err := getStorageDBFlagValues(c.StringSlice(flagStorageLBaseMaxBytes.Name))
	if err != nil {
		return nil, err
	}
	lbaseMaxBytes, err := mapf(lbaseMaxBytesStr[:], func(s string) (int64, error) {
		return units.FromByteSizeString(s)
	})
	if err != nil {
		return nil, err
	}
	memTableSizeStr, err := getStorageDBFlagValues(c.StringSlice(flagStorageMemTableSize.Name))
	if err != nil {
		return nil, err
	}
	memTableSize, err := mapf(memTableSizeStr[:], func(s string) (int, error) {
		size, err := units.FromByteSizeString(s)
		return int(size), err
	})
	if err != nil {
		return nil, err
	}
	memTableStopWriteThreshold, err := getStorageDBFlagValues(c.IntSlice(flagStorageMemTableStopWritesThreshold.Name))
	if err != nil {
		return nil, err
	}
	maxConcurrentCompaction, err := getStorageDBFlagValues(c.IntSlice(flagStorageMaxConcurrentCompaction.Name))
	if err != nil {
		return nil, err
	}
	maxOpenFiles, err := getStorageDBFlagValues(c.IntSlice(flagStorageMaxOpenFiles.Name))
	if err != nil {
		return nil, err
	}

	getStorageDBOptions := func(i int) []storage.StoreOption {
		return []storage.StoreOption{
			storage.WithL0CompactionFileThreshold(l0CompactionFileThreshold[i]),
			storage.WithL0CompactionThreshold(l0CompactionThreshold[i]),
			storage.WithL0StopWritesThreshold(l0StopWritesThreshold[i]),
			storage.WithL0TargetFileSize(l0TargetFileSize[i]),
			storage.WithFlushSplitBytes(flushSplitBytes[i]),
			storage.WithLBaseMaxBytes(lbaseMaxBytes[i]),
			storage.WithMaxOpenFiles(maxOpenFiles[i]),
			storage.WithMemTableSize(memTableSize[i]),
			storage.WithMemTableStopWritesThreshold(memTableStopWriteThreshold[i]),
			storage.WithMaxConcurrentCompaction(maxConcurrentCompaction[i]),
		}
	}

	valueStoreOpts := slices.Concat(commonStoreOpts, []storage.StoreOption{
		storage.WithWAL(!c.Bool(flagStorageDataDBDisableWAL.Name)),
		storage.WithSyncWAL(!c.Bool(flagStorageDataDBNoSync.Name)),
		storage.WithVerbose(c.Bool(flagStorageVerbose.Name)),
	}, getStorageDBOptions(0))

	commitStoreOpts := slices.Concat(commonStoreOpts, []storage.StoreOption{
		storage.WithWAL(!c.Bool(flagStorageCommitDBDisableWAL.Name)),
		storage.WithSyncWAL(!c.Bool(flagStorageCommitDBNoSync.Name)),
		storage.WithVerbose(c.Bool(flagStorageVerbose.Name)),
	}, getStorageDBOptions(1))

	if name := flagStorageTrimDelay.Name; c.IsSet(name) {
		valueStoreOpts = append(valueStoreOpts, storage.WithTrimDelay(c.Duration(name)))
		commitStoreOpts = append(commitStoreOpts, storage.WithTrimDelay(c.Duration(name)))
	}

	if name := flagStorageTrimRate.Name; c.IsSet(name) {
		rate, err := units.FromByteSizeString(c.String(name))
		if err != nil {
			return nil, err
		}
		valueStoreOpts = append(valueStoreOpts, storage.WithTrimRateByte(int(rate)))
		commitStoreOpts = append(commitStoreOpts, storage.WithTrimRateByte(int(rate)))
	}

	opts = []storage.Option{
		storage.WithValueStoreOptions(valueStoreOpts...),
		storage.WithCommitStoreOptions(commitStoreOpts...),
		storage.WithMetricsLogInterval(c.Duration(flagStorageMetricsLogInterval.Name)),
	}

	return opts, nil
}

func getStorageDBFlagValues[T any](values []T) (ret [2]T, err error) {
	if len(values) == 0 {
		return ret, errors.New("no values")
	}
	if len(values) > 2 {
		return ret, errors.New("too many values")
	}
	if len(values) == 1 {
		ret[0], ret[1] = values[0], values[0]
	} else {
		ret[0], ret[1] = values[0], values[1]
	}
	return ret, nil
}

func mapf[S, T any](ss []S, f func(S) (T, error)) ([]T, error) {
	var err error
	ts := make([]T, len(ss))
	for i := range ss {
		ts[i], err = f(ss[i])
		if err != nil {
			return nil, err
		}
	}
	return ts, nil
}
