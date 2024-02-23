package benchmark

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/multierr"

	"golang.org/x/sync/errgroup"
)

type Benchmark struct {
	config
	loaders []*Loader
	metrics Metrics
	stopC   chan struct{}
}

// New creates a new Benchmark and returns it. Users must call Close to release resources if it returns successfully.
func New(opts ...Option) (bm *Benchmark, err error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	bm = &Benchmark{
		config: cfg,
		stopC:  make(chan struct{}),
	}

	defer func() {
		if err != nil {
			_ = bm.Close()
		}
	}()

	for idx := range bm.targets {
		var loader *Loader
		target := bm.targets[idx]
		loaderMetrics := &LoaderMetrics{tgt: target}
		loader, err = NewLoader(loaderConfig{
			Target:              target,
			cid:                 bm.cid,
			mraddrs:             bm.mraddrs,
			metrics:             loaderMetrics,
			singleConnPerTarget: bm.singleConnPerTarget,
			stopC:               bm.stopC,
		})
		if err != nil {
			return bm, err
		}
		bm.loaders = append(bm.loaders, loader)
		bm.metrics.loaderMetrics = append(bm.metrics.loaderMetrics, loaderMetrics)
	}

	return bm, nil
}

// Run starts Loaders and metric reporter. It blocks until the loaders are finished.
func (bm *Benchmark) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	benchmarkTimer := time.NewTimer(bm.duration)

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)

	reportTick := time.NewTicker(bm.reportInterval)
	defer reportTick.Stop()

	var finished bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			close(bm.stopC)
			wg.Done()
			fmt.Println(MustEncode(bm.reportEncoder, bm.metrics.Flush()))
			cancel()
		}()
		for {
			select {
			case <-benchmarkTimer.C:
				finished = true
				slog.Debug("benchmark is finished")
				return
			case sig := <-sigC:
				finished = true
				slog.Debug("caught signal", slog.String("signal", sig.String()))
				return
			case <-ctx.Done():
				slog.Debug("loader failed")
				return
			case <-reportTick.C:
				fmt.Println(MustEncode(bm.reportEncoder, bm.metrics.Flush()))
			}
		}
	}()

	for _, tw := range bm.loaders {
		tw := tw
		g.Go(func() error {
			return tw.Run(ctx)
		})
	}
	err := g.Wait()
	wg.Wait()

	slog.Debug("stopped benchmark")
	if finished {
		return nil
	}
	return err
}

// Close releases resources acquired by Benchmark and Loader.
func (bm *Benchmark) Close() error {
	var err error
	for _, loader := range bm.loaders {
		err = multierr.Append(err, loader.Close())
	}
	return err
}
