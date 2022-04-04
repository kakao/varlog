package storagenode

import (
	"fmt"
	"io"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/logstream"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
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

func (rcs reportCommitServer) Commit(stream snpb.LogStreamReporter_CommitServer) (err error) {
	defer func() {
		err = multierr.Append(err, stream.SendAndClose(&snpb.CommitResponse{}))
		rcs.sn.logger.Info("report commit server: closed commit stream", zap.Error(err))
	}()

	// NOTE: Each connection of Commit is paired with only one log stream replica. Thus, lse can be safely cached.
	lseMap := make(map[types.LogStreamID]*logstream.Executor, defaultReportsCapacity)
	req := &snpb.CommitRequest{}
	ctx := stream.Context()
	for {
		err = stream.RecvMsg(req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		tpid, lsid := req.CommitResult.TopicID, req.CommitResult.LogStreamID
		lse, loaded := lseMap[lsid]
		if !loaded {
			lse, loaded = rcs.sn.executors.Load(tpid, lsid)
			if !loaded {
				return fmt.Errorf("storage node: no such log stream executor %d, %d", req.CommitResult.TopicID, req.CommitResult.LogStreamID)
			}
			lseMap[lsid] = lse
		}
		_ = lse.Commit(ctx, req.CommitResult)
	}
}
