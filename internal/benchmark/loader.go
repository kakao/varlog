package benchmark

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/varlogpb"
)

type loaderConfig struct {
	Target
	cid     types.ClusterID
	mraddrs []string
	metrics *LoaderMetrics
}

type Loader struct {
	loaderConfig
	batch [][]byte
	apps  []varlog.Log
	subs  []varlog.Log
	begin struct {
		lsn varlogpb.LogSequenceNumber
		ch  chan varlogpb.LogSequenceNumber
	}
	logger *slog.Logger
}

func NewLoader(cfg loaderConfig) (loader *Loader, err error) {
	loader = &Loader{
		loaderConfig: cfg,
	}
	loader.begin.ch = make(chan varlogpb.LogSequenceNumber, cfg.AppendersCount)
	loader.logger = slog.With(slog.Any("topic", loader.TopicID))

	defer func() {
		if err != nil {
			_ = loader.Close()
		}
	}()

	var c varlog.Log
	for i := uint(0); i < loader.AppendersCount; i++ {
		c, err = varlog.Open(context.TODO(), loader.cid, loader.mraddrs)
		if err != nil {
			return loader, err
		}
		loader.apps = append(loader.apps, c)
	}

	for i := uint(0); i < loader.SubscribersCount; i++ {
		c, err = varlog.Open(context.TODO(), loader.cid, loader.mraddrs)
		if err != nil {
			return loader, err
		}
		loader.subs = append(loader.subs, c)
	}

	msg := NewMessage(loader.MessageSize)
	loader.batch = make([][]byte, loader.BatchSize)
	for i := range loader.batch {
		loader.batch[i] = msg
	}

	loader.logger.Debug("created topic worker")
	return loader, nil
}

// Run starts goroutines that append log entries and subscribe to them.
func (loader *Loader) Run(ctx context.Context) (err error) {
	loader.metrics.Reset(time.Now())

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	defer func() {
		err = multierr.Append(err, g.Wait())
		cancel()
	}()

	for i := 0; i < len(loader.apps); i++ {
		c := loader.apps[i]
		g.Go(func() error {
			return loader.appendLoop(ctx, c)
		})
	}

	err = loader.setBeginLSN(ctx)
	if err != nil {
		err = fmt.Errorf("begin lsn: %w", err)
		return err
	}

	for i := 0; i < len(loader.subs); i++ {
		c := loader.subs[i]
		g.Go(func() error {
			return loader.subscribeLoop(ctx, c)
		})
	}

	return err
}

func (loader *Loader) Close() error {
	var err error
	for _, c := range loader.apps {
		err = multierr.Append(err, c.Close())
	}
	for _, c := range loader.subs {
		err = multierr.Append(err, c.Close())
	}
	return err
}

func (loader *Loader) appendLoop(ctx context.Context, c varlog.Log) error {
	begin := true
	var am AppendMetrics
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ts := time.Now()
		var res varlog.AppendResult
		if loader.LogStreamID.Invalid() {
			res = c.Append(ctx, loader.TopicID, loader.batch)
		} else {
			res = c.AppendTo(ctx, loader.TopicID, loader.LogStreamID, loader.batch)
		}
		dur := time.Since(ts)
		if res.Err != nil {
			return fmt.Errorf("append: %w", res.Err)
		}
		if begin {
			loader.begin.ch <- varlogpb.LogSequenceNumber{
				LLSN: res.Metadata[0].LLSN,
				GLSN: res.Metadata[0].GLSN,
			}
			begin = false
		}
		cnt := len(res.Metadata)
		am.bytes += int64(loader.BatchSize * loader.MessageSize)
		am.requests++
		am.durationMS += dur.Milliseconds()
		if loader.metrics.ReportAppendMetrics(am) {
			am = AppendMetrics{}
		}

		loader.logger.Debug("append",
			slog.Int("count", cnt),
			slog.Any("logstream", res.Metadata[0].LogStreamID),
			slog.Any("firstGLSN", res.Metadata[0].GLSN),
			slog.Any("lastGLSN", res.Metadata[cnt-1].GLSN),
			slog.Any("firstLLSN", res.Metadata[0].LLSN),
			slog.Any("lastLLSN", res.Metadata[cnt-1].LLSN),
		)
	}
}

func (loader *Loader) subscribeLoop(ctx context.Context, c varlog.Log) error {
	var sm SubscribeMetrics
	if loader.LogStreamID.Invalid() {
		var subErr error
		stop := make(chan struct{})
		closer, err := c.Subscribe(ctx, loader.TopicID, loader.begin.lsn.GLSN, types.MaxGLSN, func(logEntry varlogpb.LogEntry, err error) {
			if err != nil {
				subErr = err
				close(stop)
				return
			}
			loader.logger.Debug("subscribed", zap.String("log", logEntry.String()))
			sm.logs++
			sm.bytes += int64(len(logEntry.Data))
			if loader.metrics.ReportSubscribeMetrics(sm) {
				sm = SubscribeMetrics{}
			}
		})
		if err != nil {
			return err
		}
		defer closer()

		select {
		case <-stop:
			if subErr != nil {
				return fmt.Errorf("subscribe: %w", subErr)
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	subscriber := c.SubscribeTo(ctx, loader.TopicID, loader.LogStreamID, loader.begin.lsn.LLSN, types.MaxLLSN)
	defer func() {
		_ = subscriber.Close()
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		logEntry, err := subscriber.Next()
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
		loader.logger.Debug("subscribeTo", slog.Any("llsn", logEntry.LLSN))
		sm.logs++
		sm.bytes += int64(len(logEntry.Data))
		if loader.metrics.ReportSubscribeMetrics(sm) {
			sm = SubscribeMetrics{}
		}
	}
}

func (loader *Loader) setBeginLSN(ctx context.Context) error {
	beginLSN := varlogpb.LogSequenceNumber{
		LLSN: types.MaxLLSN,
		GLSN: types.MaxGLSN,
	}
	for i := uint(0); i < loader.AppendersCount; i++ {
		select {
		case lsn := <-loader.begin.ch:
			if lsn.GLSN < beginLSN.GLSN {
				beginLSN = lsn
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	loader.begin.lsn = beginLSN
	loader.logger.Debug("begin lsn", slog.Any("lsn", beginLSN))
	return nil
}
