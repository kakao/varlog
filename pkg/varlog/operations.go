package varlog

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/kakao/varlog/pkg/logc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

// TODO: use ops-accumulator?
func (v *logImpl) append(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data []byte, opts ...AppendOption) (meta varlogpb.LogEntryMeta, err error) {
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
		var currErr error
		if appendOpts.selectLogStream {
			if logStreamID, ok = v.lsSelector.Select(topicID); !ok {
				err = multierr.Append(err, errors.New("no usable log stream"))
				continue
			}
		}
		replicas, ok = v.replicasRetriever.Retrieve(topicID, logStreamID)
		if !ok {
			err = multierr.Append(err, errors.New("no such log stream replicas"))
			continue
		}
		primarySNID = replicas[0].GetStorageNodeID()
		primaryLogCL, currErr = v.logCLManager.GetOrConnect(ctx, primarySNID, replicas[0].GetAddress())
		if currErr != nil {
			err = multierr.Append(err, currErr)
			v.allowlist.Deny(topicID, logStreamID)
			continue
		}
		snList := make([]varlogpb.StorageNode, len(replicas)-1)
		for i := range replicas[1:] {
			snList[i].Address = replicas[i+1].GetAddress()
			snList[i].StorageNodeID = replicas[i+1].GetStorageNodeID()
		}
		res, currErr := primaryLogCL.Append(ctx, topicID, logStreamID, [][]byte{data}, snList...)
		if currErr != nil {
			replicasInfo := make([]string, 0, len(replicas))
			for _, replica := range replicas {
				replicasInfo = append(replicasInfo, replica.String())
			}
			err = multierr.Append(err, errors.Wrapf(currErr, "varlog: append (snid=%d, lsid=%d, replicas=%s)", primarySNID, logStreamID, strings.Join(replicasInfo, ", ")))
			// FIXME (jun): It affects other goroutines that are doing I/O.
			// Close a client only when err is related to the connection.
			primaryLogCL.Close()
			v.allowlist.Deny(topicID, logStreamID)
			continue
		}
		meta = res[0].Meta
		return meta, nil
	}
	return meta, err
}

func (v *logImpl) read(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, glsn types.GLSN) (varlogpb.LogEntry, error) {
	replicas, ok := v.replicasRetriever.Retrieve(topicID, logStreamID)
	if !ok {
		return varlogpb.InvalidLogEntry(), errNoLogStream
	}
	primarySNID := replicas[0].GetStorageNodeID()
	primaryLogCL, err := v.logCLManager.GetOrConnect(ctx, primarySNID, replicas[0].GetAddress())
	if err != nil {
		return varlogpb.InvalidLogEntry(), errNoLogIOClient
	}
	// FIXME (jun
	// 1) LogEntry -> non-nullable field
	// 2) deepcopy LogEntry
	logEntry, err := primaryLogCL.Read(ctx, topicID, logStreamID, glsn)
	if err != nil {
		return varlogpb.InvalidLogEntry(), err
	}
	return *logEntry, nil
}
