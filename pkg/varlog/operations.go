package varlog

import (
	"context"
	"fmt"
	"strings"
	"sync"

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
			if _, ok = appendOpts.allowedLogStreams[lsid]; appendOpts.allowedLogStreams != nil && !ok {
				err := fmt.Errorf("append: not allowed lsid %d", lsid)
				result.Err = multierr.Append(result.Err, err)

				v.allowlist.Deny(tpid, lsid)
				i--
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

func (v *logImpl) peekLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (first varlogpb.LogSequenceNumber, last varlogpb.LogSequenceNumber, err error) {
	replicas, ok := v.replicasRetriever.Retrieve(tpid, lsid)
	if !ok {
		err = errNoLogStream
		return
	}

	var (
		errs  = make([]error, len(replicas))
		wg    sync.WaitGroup
		mu    sync.Mutex
		found bool
	)
	for idx := range replicas {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			client, erri := v.logCLManager.GetOrConnect(ctx, replicas[idx].StorageNodeID, replicas[idx].Address)
			if erri != nil {
				errs[idx] = erri
				return
			}
			lsrmd, erri := client.LogStreamReplicaMetadata(ctx, tpid, lsid)
			if erri != nil {
				errs[idx] = erri
				return
			}
			switch lsrmd.Status {
			case varlogpb.LogStreamStatusRunning, varlogpb.LogStreamStatusSealed:
				mu.Lock()
				defer mu.Unlock()
				if first.LLSN < lsrmd.LocalLowWatermark.LLSN {
					first = lsrmd.LocalLowWatermark
				}
				if last.LLSN < lsrmd.LocalHighWatermark.LLSN {
					last = lsrmd.LocalHighWatermark
				}
				found = true
			default:
				errs[idx] = fmt.Errorf("logstream replica snid=%v: invalid status: %s",
					replicas[idx].StorageNodeID, lsrmd.Status,
				)
			}
		}(idx)
	}
	wg.Wait()

	if found {
		return first, last, nil
	}
	err = multierr.Combine(errs...)
	return first, last, err
}
