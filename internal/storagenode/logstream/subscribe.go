package logstream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type SubscribeResult struct {
	c       chan varlogpb.LogEntry
	decider *decidableCondition
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	err     error
}

func (lse *Executor) newSubscribeResult() (*SubscribeResult, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	sr := &SubscribeResult{
		c:       make(chan varlogpb.LogEntry),
		decider: lse.decider,
		cancel:  cancel,
	}
	return sr, ctx
}

// Result returns the channel of the result.
func (sr *SubscribeResult) Result() <-chan varlogpb.LogEntry {
	return sr.c
}

// Stop stops the subscription.
// This should be called to release resources after using SubscribeResult.
func (sr *SubscribeResult) Stop() {
	sr.stop()
	sr.wg.Wait()
}

// Err returns the error of the subscription.
// This should be called after Stop is called.
func (sr *SubscribeResult) Err() error {
	err := sr.err
	return err
}

func (sr *SubscribeResult) stop() {
	sr.decider.change(func() {
		sr.cancel()
	})
}

// SubscribeWithGLSN subscribes to the log stream with the given range of GLSNs.
// TODO: The first argument ctx may not be necessary, since the subscription can be stopped by the `internal/varlogsn/logstream.(*SubscribeResult).Stop()`.
func (lse *Executor) SubscribeWithGLSN(begin, end types.GLSN) (*SubscribeResult, error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	if lse.esm.load() == executorStateClosed {
		return nil, verrors.ErrClosed
	}

	if begin >= end {
		return nil, fmt.Errorf("log stream: invalid range: %w", verrors.ErrInvalid)
	}

	lse.globalLowWatermark.mu.Lock()
	if begin < lse.globalLowWatermark.glsn {
		lse.globalLowWatermark.mu.Unlock()
		return nil, fmt.Errorf("log stream: %w", verrors.ErrTrimmed)
	}
	lse.globalLowWatermark.mu.Unlock()

	sr, ctx := lse.newSubscribeResult()
	sr.wg.Add(1)
	go func() {
		defer sr.wg.Done()
		sr.err = lse.scanWithGLSN(ctx, begin, end, sr)
	}()
	return sr, nil
}

// SubscribeWithLLSN subscribes to the log stream with the given range of LLSNs.
// TODO: The first argument ctx may not be necessary, since the subscription can be stopped by the `internal/varlogsn/logstream.(*SubscribeResult).Stop()`.
func (lse *Executor) SubscribeWithLLSN(begin, end types.LLSN) (*SubscribeResult, error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	if lse.esm.load() == executorStateClosed {
		return nil, verrors.ErrClosed
	}

	if begin >= end {
		return nil, fmt.Errorf("log stream: invalid range: %w", verrors.ErrInvalid)
	}

	if begin < lse.lsc.localLowWatermark().LLSN {
		return nil, fmt.Errorf("log stream: %w", verrors.ErrTrimmed)
	}

	sr, ctx := lse.newSubscribeResult()
	sr.wg.Add(1)
	go func() {
		defer sr.wg.Done()
		sr.err = lse.scanWithLLSN(ctx, begin, end, sr)
	}()
	return sr, nil
}

func (lse *Executor) scanWithGLSN(ctx context.Context, begin, end types.GLSN, sr *SubscribeResult) error {
	defer close(sr.c)
	scanBegin := begin
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, globalHWM, _ := lse.lsc.reportCommitBase()

		lastGLSN := types.InvalidGLSN
		scanner := lse.stg.NewScanner(storage.WithGLSN(scanBegin, end))
		for scanner.Valid() {
			le, err := scanner.Value()
			if err != nil {
				_ = scanner.Close()
				return err
			}
			le.TopicID = lse.tpid
			le.LogStreamID = lse.lsid
			lastGLSN = le.GLSN
			select {
			case sr.c <- le:
			case <-ctx.Done():
				_ = scanner.Close()
				return nil
			}
			_ = scanner.Next()
		}
		_ = scanner.Close()
		if lastGLSN == end-1 {
			return nil
		}
		if !lastGLSN.Invalid() {
			scanBegin = lastGLSN + 1
		}

		if err := lse.decider.waitC(ctx, globalHWM+1); err != nil {
			return err
		}
	}
}

func (lse *Executor) scanWithLLSN(ctx context.Context, begin, end types.LLSN, sr *SubscribeResult) error {
	defer close(sr.c)
	scanBegin := begin
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, globalHWM, _ := lse.lsc.reportCommitBase()
		localHWM := lse.lsc.localHighWatermark()
		scanEnd := end
		if localHWM.LLSN+1 < scanEnd {
			scanEnd = localHWM.LLSN + 1
		}

		lastLLSN := types.InvalidLLSN
		scanner := lse.stg.NewScanner(storage.WithLLSN(scanBegin, scanEnd))
		for scanner.Valid() {
			le, err := scanner.Value()
			if err != nil {
				_ = scanner.Close()
				return err
			}
			le.TopicID = lse.tpid
			le.LogStreamID = lse.lsid
			lastLLSN = le.LLSN
			select {
			case sr.c <- le:
			case <-ctx.Done():
				_ = scanner.Close()
				return nil
			}
			_ = scanner.Next()
		}
		_ = scanner.Close()
		if lastLLSN == end-1 {
			return nil
		}
		if !lastLLSN.Invalid() {
			scanBegin = lastLLSN + 1
		}

		if err := lse.decider.waitC(ctx, globalHWM+1); err != nil {
			return err
		}
	}
}
