package executor

import (
	"context"
	stderrors "errors"

	"go.opentelemetry.io/otel/attribute"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
)

var errOldCommit = stderrors.New("too old commit result")

func (e *executor) GetReport() (snpb.LogStreamUncommitReport, error) {
	if err := e.guard(); err != nil {
		return snpb.InvalidLogStreamUncommitReport, err
	}
	defer e.unguard()

	version, highWatermark, uncommittedLLSNBegin := e.lsc.reportCommitBase()
	uncommittedLLSNEnd := e.lsc.uncommittedLLSNEnd.Load()

	e.metrics.Reports.Add(context.TODO(), 1, attribute.Int64("lsid", int64(e.logStreamID)))
	e.metrics.ReportedLogEntries.Record(context.TODO(), int64(uncommittedLLSNEnd-uncommittedLLSNBegin), attribute.Int64("lsid", int64(e.logStreamID)))

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
