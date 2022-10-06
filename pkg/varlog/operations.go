package varlog

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

// TODO: use ops-accumulator?
func (v *logImpl) append(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, data [][]byte, opts ...AppendOption) (result AppendResult) {
	appendOpts := defaultAppendOptions()
	for _, opt := range opts {
		opt.apply(&appendOpts)
	}

	for i := 0; i < appendOpts.retryCount+1; i++ {
		if appendOpts.selectLogStream {
			var ok bool
			if lsid, ok = v.lsSelector.Select(tpid); !ok {
				err := fmt.Errorf("append: no usable log stream in topic %d", tpid)
				result.Err = multierr.Append(result.Err, err)
				continue
			}
			if _, ok = appendOpts.allowLogStreams[lsid]; appendOpts.allowLogStreams != nil && !ok {
				err := fmt.Errorf("append: not allowed lsid %d", lsid)
				result.Err = multierr.Append(result.Err, err)
				continue
			}
		}
		res, err := v.appendTo(ctx, tpid, lsid, data)
		if err != nil {
			result.Err = err
			continue
		}
		result.Err = nil
		for idx := 0; idx < len(res); idx++ {
			if len(res[idx].Error) > 0 {
				if strings.Contains(err.Error(), "sealed") {
					result.Err = fmt.Errorf("append: %s: %w", res[idx].Error, verrors.ErrSealed)
				} else {
					result.Err = fmt.Errorf("append: %s", res[idx].Error)
				}
				break
			}
			result.Metadata = append(result.Metadata, res[idx].Meta)
		}
		break
	}
	return result
}

func (v *logImpl) appendTo(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, data [][]byte) ([]snpb.AppendResult, error) {
	replicas, ok := v.replicasRetriever.Retrieve(tpid, lsid)
	if !ok {
		return nil, fmt.Errorf("append: log stream %d of topic %d does not exist", lsid, tpid)
	}
	snid := replicas[0].StorageNodeID
	addr := replicas[0].Address
	cl, err := v.logCLManager.GetOrConnect(ctx, snid, addr)
	if err != nil {
		// add deny list
		v.allowlist.Deny(tpid, lsid)
		return nil, fmt.Errorf("append: %w", err)
	}

	backup := make([]varlogpb.StorageNode, len(replicas)-1)
	for i := 0; i < len(replicas)-1; i++ {
		backup[i].StorageNodeID = replicas[i+1].StorageNodeID
		backup[i].Address = replicas[i+1].Address
	}

	res, err := cl.Append(ctx, tpid, lsid, data, backup...)
	if err != nil {
		if strings.Contains(err.Error(), "sealed") {
			err = fmt.Errorf("append: %s: %w", err.Error(), verrors.ErrSealed)
		}

		// FIXME: Do not close clients. Let gRPC manages the connection.
		// _ = cl.Close()

		// add deny list
		v.allowlist.Deny(tpid, lsid)

		return nil, err
	}

	return res, nil
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
		if lsd.Status.Deleted() {
			err = multierr.Append(err, errors.Errorf("invalid status: %s", lsd.Status.String()))
			continue
		}
		return lsd, nil
	}
	return lsd, err
}

func (v *logImpl) logStreamReplicaMetadata(ctx context.Context, tpID types.TopicID, lsID types.LogStreamID) (snpb.LogStreamReplicaMetadataDescriptor, error) {
	replicas, ok := v.replicasRetriever.Retrieve(tpID, lsID)
	if !ok {
		return snpb.LogStreamReplicaMetadataDescriptor{}, errNoLogStream
	}

	var err error
	for _, replica := range replicas {
		cl, cerr := v.logCLManager.GetOrConnect(ctx, replica.StorageNodeID, replica.Address)
		if cerr != nil {
			err = multierr.Append(err, cerr)
			continue
		}

		lsrmd, cerr := cl.LogStreamReplicaMetadata(ctx, tpID, lsID)
		if cerr != nil {
			err = multierr.Append(err, cerr)
			continue
		}
		return lsrmd, nil
	}
	return snpb.LogStreamReplicaMetadataDescriptor{}, err
}
