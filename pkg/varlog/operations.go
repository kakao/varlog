package varlog

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/pkg/logc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// TODO: use ops-accumulator?
func (v *logImpl) append(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data [][]byte, opts ...AppendOption) (result AppendResult) {
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
		var err error
		if appendOpts.selectLogStream {
			if logStreamID, ok = v.lsSelector.Select(topicID); !ok {
				result.Err = multierr.Append(result.Err, errors.New("no usable log stream"))
				continue
			}
		}
		replicas, ok = v.replicasRetriever.Retrieve(topicID, logStreamID)
		if !ok {
			result.Err = multierr.Append(result.Err, errors.New("no such log stream replicas"))
			continue
		}
		primarySNID = replicas[0].GetStorageNodeID()
		primaryLogCL, err = v.logCLManager.GetOrConnect(ctx, primarySNID, replicas[0].GetAddress())
		if err != nil {
			result.Err = multierr.Append(result.Err, err)
			v.allowlist.Deny(topicID, logStreamID)
			continue
		}
		snList := make([]varlogpb.StorageNode, len(replicas)-1)
		for i := range replicas[1:] {
			snList[i].Address = replicas[i+1].GetAddress()
			snList[i].StorageNodeID = replicas[i+1].GetStorageNodeID()
		}
		res, err := primaryLogCL.Append(ctx, topicID, logStreamID, data, snList...)
		if err != nil {
			if strings.Contains(err.Error(), "sealed") {
				err = errors.Wrap(verrors.ErrSealed, err.Error())
			}

			replicasInfo := make([]string, 0, len(replicas))
			for _, replica := range replicas {
				replicasInfo = append(replicasInfo, replica.String())
			}
			result.Err = multierr.Append(result.Err, errors.Wrapf(err, "varlog: append (snid=%d, lsid=%d, replicas=%s)", primarySNID, logStreamID, strings.Join(replicasInfo, ", ")))
			// FIXME (jun): It affects other goroutines that are doing I/O.
			// Close a client only when err is related to the connection.
			primaryLogCL.Close()
			v.allowlist.Deny(topicID, logStreamID)
			continue
		}
		result.Err = nil
		for idx := 0; idx < len(res); idx++ {
			if len(res[idx].Error) > 0 {
				if strings.Contains(res[idx].Error, "sealed") {
					result.Err = errors.Wrap(verrors.ErrSealed, res[idx].Error)
				} else {
					result.Err = errors.New(res[idx].Error)
				}
				break
			}
			result.Metadata = append(result.Metadata, res[idx].Meta)
		}
		return result
	}
	return result
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

func (v *logImpl) logStreamMetadata(ctx context.Context, tpID types.TopicID, lsID types.LogStreamID) (lsd varlogpb.LogStreamDescriptor, err error) {
	replicas, ok := v.replicasRetriever.Retrieve(tpID, lsID)
	if !ok {
		return varlogpb.LogStreamDescriptor{}, errNoLogStream
	}

	for _, replica := range replicas {
		cl, cerr := v.logCLManager.GetOrConnect(ctx, replica.StorageNodeID, replica.Address)
		if cerr != nil {
			err = multierr.Append(err, cerr)
			continue
		}
		lsd, cerr = cl.LogStreamMetadata(ctx, tpID, lsID)
		if cerr != nil {
			err = multierr.Append(err, cerr)
			continue
		}
		if !lsd.Status.Running() {
			err = multierr.Append(err, errors.Errorf("invalid status: %s", lsd.Status.String()))
			continue
		}
		return lsd, nil
	}
	return lsd, err
}
