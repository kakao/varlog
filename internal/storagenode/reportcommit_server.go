package storagenode

import (
	"fmt"
	"io"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
)

const defaultReportsCapacity = 32

type reportCommitServer struct {
	sn *StorageNode
}

var _ snpb.LogStreamReporterServer = (*reportCommitServer)(nil)

func (rcs reportCommitServer) GetReport(stream snpb.LogStreamReporter_GetReportServer) (err error) {
	defer func() {
		rcs.sn.logger.Info("report commit server: closed report stream", zap.Error(err))
	}()

	req := &snpb.GetReportRequest{}
	rsp := &snpb.GetReportResponse{
		StorageNodeID:   rcs.sn.snid,
		UncommitReports: make([]snpb.LogStreamUncommitReport, 0, defaultReportsCapacity),
	}
	ctx := stream.Context()
	for {
		req.Reset()
		err = stream.RecvMsg(req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		rsp.UncommitReports = rsp.UncommitReports[0:0]
		// NOTE: Each connection of GetReport is paired with only one log stream replica.
		// So we can cache log stream executor.
		rcs.sn.executors.Range(func(_ types.LogStreamID, _ types.TopicID, lse *logstream.Executor) bool {
			if report, err := lse.Report(ctx); err == nil {
				rsp.UncommitReports = append(rsp.UncommitReports, report)
			}
			return true
		})

		err = stream.SendMsg(rsp)
		if err != nil {
			return err
		}
	}
}

func (rcs reportCommitServer) CommitBatch(stream snpb.LogStreamReporter_CommitBatchServer) (err error) {
	defer func() {
		err = multierr.Append(err, stream.SendAndClose(&snpb.CommitBatchResponse{}))
		rcs.sn.logger.Info("report commit server: closed commit stream", zap.Error(err))
	}()

	req := &snpb.CommitBatchRequest{}
	ctx := stream.Context()
	for {
		commitResults := req.CommitResults
		commitResults = commitResults[:0]
		req.Reset()
		req.CommitResults = commitResults

		err = stream.RecvMsg(req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		for _, cr := range req.CommitResults {
			tpid, lsid := cr.TopicID, cr.LogStreamID

			lse, loaded := rcs.sn.executors.Load(tpid, lsid)
			if !loaded {
				return fmt.Errorf("storage node: no such log stream executor %d, %d", cr.TopicID, cr.LogStreamID)
			}
			_ = lse.Commit(ctx, cr)
		}
	}
}
