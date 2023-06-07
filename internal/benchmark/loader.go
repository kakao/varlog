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

func (loader *Loader) makeAppendFunc(ctx context.Context, c varlog.Log, am *AppendMetrics) (appendFunc func(), closeFunc func()) {
	begin := true
	notifyBegin := func(meta varlogpb.LogEntryMeta) {
		loader.begin.ch <- varlogpb.LogSequenceNumber{
			LLSN: meta.LLSN,
			GLSN: meta.GLSN,
		}
		begin = false
	}

	debugLog := func(meta []varlogpb.LogEntryMeta) {
		cnt := len(meta)
		loader.logger.Debug("append",
			slog.Int("count", cnt),
			slog.Any("logstream", meta[0].LogStreamID),
			slog.Any("firstGLSN", meta[0].GLSN),
			slog.Any("lastGLSN", meta[cnt-1].GLSN),
			slog.Any("firstLLSN", meta[0].LLSN),
			slog.Any("lastLLSN", meta[cnt-1].LLSN),
		)
	}

	recordMetrics := func(dur time.Duration) {
		am.bytes += int64(loader.BatchSize * loader.MessageSize)
		am.requests++
		am.durationMS += dur.Milliseconds()
		if loader.metrics.ReportAppendMetrics(*am) {
			*am = AppendMetrics{}
		}
	}

	tpid, lsid := loader.TopicID, loader.LogStreamID

	closeFunc = func() {}
	if lsid.Invalid() {
		return func() {
			ts := time.Now()
			res := c.Append(ctx, loader.TopicID, loader.batch)
			if res.Err != nil {
				panic(res.Err)
			}
			dur := time.Since(ts)
			recordMetrics(dur)
			if begin {
				notifyBegin(res.Metadata[0])
			}
			debugLog(res.Metadata)
		}, closeFunc
	}

	if loader.PipelineSize == 0 {
		return func() {
			ts := time.Now()
			res := c.AppendTo(ctx, tpid, lsid, loader.batch)
			if res.Err != nil {
				panic(res.Err)
			}
			dur := time.Since(ts)
			recordMetrics(dur)
			if begin {
				notifyBegin(res.Metadata[0])
			}
			debugLog(res.Metadata)
		}, closeFunc
	}

	lsa, err := c.NewLogStreamAppender(loader.TopicID, loader.LogStreamID,
		varlog.WithPipelineSize(loader.PipelineSize),
	)
	if err != nil {
		panic(err)
	}
	closeFunc = lsa.Close
	return func() {
		ts := time.Now()
		err := lsa.AppendBatch(loader.batch, func(lem []varlogpb.LogEntryMeta, err error) {
			if err != nil {
				panic(err)
			}
			dur := time.Since(ts)
			recordMetrics(dur)
			if begin {
				notifyBegin(lem[0])
			}
			debugLog(lem)
		})
		if err != nil {
			panic(err)
		}
	}, closeFunc
}

func (loader *Loader) appendLoop(ctx context.Context, c varlog.Log) error {
	var am AppendMetrics
	appendFunc, closeFunc := loader.makeAppendFunc(ctx, c, &am)
	defer closeFunc()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			appendFunc()
		}
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
