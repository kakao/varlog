package executor

import (
	"context"
	stderrors "errors"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

var errOldCommit = stderrors.New("too old commit result")

func (e *executor) GetReport() (snpb.LogStreamUncommitReport, error) {
	if err := e.guard(); err != nil {
		return snpb.InvalidLogStreamUncommitReport, err
	}
	defer e.unguard()

	e.metrics.Reports.Add(context.TODO(), 1, attribute.Int64("lsid", int64(e.logStreamID)))

	version, highWatermark, uncommittedLLSNBegin := e.lsc.reportCommitBase()
	uncommittedLLSNEnd := e.lsc.uncommittedLLSNEnd.Load()
	e.metrics.ReportedLogEntries.Record(context.TODO(), int64(uncommittedLLSNEnd-uncommittedLLSNBegin))

	// trace
	if uncommittedLLSNBegin%100 < e.metrics.WriteReportDelaySamplingPercentage {
		if ts, ok := e.metrics.WrittenTimestamps.LoadAndDelete(uncommittedLLSNBegin); ok {
			elapsed := time.Since(ts.(time.Time))
			e.metrics.WriteReportDelay.Record(context.TODO(), float64(elapsed.Microseconds())/1000.0, attribute.Int64("lsid", int64(e.logStreamID)))
		}
	}

	return snpb.LogStreamUncommitReport{
		LogStreamID:           e.logStreamID,
		Version:               version,
		HighWatermark:         highWatermark,
		UncommittedLLSNOffset: uncommittedLLSNBegin,
		UncommittedLLSNLength: uint64(uncommittedLLSNEnd - uncommittedLLSNBegin),
	}, nil
}

func (e *executor) Commit(ctx context.Context, commitResult snpb.LogStreamCommitResult) error {
	if err := e.guard(); err != nil {
		return err
	}
	defer e.unguard()

	e.metrics.Commits.Add(context.TODO(), 1, attribute.Int64("lsid", int64(e.logStreamID)))

	// TODO: check validate logic again
	version, _, _ := e.lsc.reportCommitBase()
	if commitResult.Version <= version {
		// too old
		// return errors.New("too old commit result")
		return errOldCommit
	}

	ct := newCommitTask()
	ct.version = commitResult.Version
	ct.highWatermark = commitResult.HighWatermark
	ct.committedGLSNBegin = commitResult.CommittedGLSNOffset
	ct.committedGLSNEnd = commitResult.CommittedGLSNOffset + types.GLSN(commitResult.CommittedGLSNLength)
	ct.committedLLSNBegin = commitResult.CommittedLLSNOffset
	if err := e.committer.sendCommitTask(ctx, ct); err != nil {
		ct.release()
		return err
	}
	return nil
}
