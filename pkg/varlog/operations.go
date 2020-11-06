package varlog

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/logc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// TODO: use ops-accumulator?
func (v *varlog) append(ctx context.Context, logStreamID types.LogStreamID, data []byte, opts AppendOptions) (glsn types.GLSN, err error) {
	var (
		replicas     []varlogpb.LogStreamReplicaDescriptor
		primaryLogCL logc.LogIOClient
		primarySNID  types.StorageNodeID
	)
	for i := 0; i < opts.Retry+1; i++ {
		var ok bool
		if opts.selectLogStream {
			if logStreamID, ok = v.lsSelector.Select(); !ok {
				continue
			}
		}
		replicas, ok = v.replicasRetriever.Retrieve(logStreamID)
		if !ok {
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
			primaryLogCL.Close()
			v.allowlist.Deny(logStreamID)
			continue
		}
	}
	return glsn, err
}

func (v *varlog) read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN, opts ReadOptions) (types.LogEntry, error) {
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
