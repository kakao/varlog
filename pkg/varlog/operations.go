package varlog

import (
	"context"
	"errors"

	"github.com/kakao/varlog/pkg/logc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

// TODO: use ops-accumulator?
func (v *varlog) append(ctx context.Context, logStreamID types.LogStreamID, data []byte, opts ...AppendOption) (glsn types.GLSN, err error) {
	appendOpts := defaultAppendOptions()
	for _, opt := range opts {
		opt.apply(&appendOpts)
	}

	var (
		replicas     []varlogpb.LogStreamReplicaDescriptor
		primaryLogCL logc.LogIOClient
		primarySNID  types.StorageNodeID
	)
	for i := 0; i < appendOpts.retryCount+1; i++ {
		var ok bool
		if appendOpts.selectLogStream {
			if logStreamID, ok = v.lsSelector.Select(); !ok {
				err = errors.New("no usable log stream")
				continue
			}
		}
		replicas, ok = v.replicasRetriever.Retrieve(logStreamID)
		if !ok {
			err = errors.New("no such log stream replicas")
			continue
		}
		primarySNID = replicas[0].GetStorageNodeID()
		primaryLogCL, err = v.logCLManager.GetOrConnect(primarySNID, replicas[0].GetAddress())
		if err != nil {
			continue
		}
		snList := make([]logc.StorageNode, len(replicas)-1)
		for i := range replicas[1:] {
			snList[i].Addr = replicas[i+1].GetAddress()
			snList[i].ID = replicas[i+1].GetStorageNodeID()
		}
		glsn, err = primaryLogCL.Append(ctx, logStreamID, data, snList...)
		if err != nil {
			// FIXME (jun): It affects other goroutines that are doing I/O.
			// Close a client only when err is related to the connection.
			primaryLogCL.Close()
			v.allowlist.Deny(logStreamID)
			continue
		}
		return glsn, err
	}
	return glsn, err
}

func (v *varlog) read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN) (types.LogEntry, error) {
	replicas, ok := v.replicasRetriever.Retrieve(logStreamID)
	if !ok {
		return types.InvalidLogEntry, errNoLogStream
	}
	primarySNID := replicas[0].GetStorageNodeID()
	primaryLogCL, err := v.logCLManager.GetOrConnect(primarySNID, replicas[0].GetAddress())
	if err != nil {
		return types.InvalidLogEntry, errNoLogIOClient
	}
	// FIXME (jun
	// 1) LogEntry -> non-nullable field
	// 2) deepcopy LogEntry
	logEntry, err := primaryLogCL.Read(ctx, logStreamID, glsn)
	if err != nil {
		return types.InvalidLogEntry, err
	}
	return *logEntry, nil
}
