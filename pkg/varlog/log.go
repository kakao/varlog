package varlog

//go:generate mockgen -package varlog -destination log_mock.go . Log

import (
	"context"
	"io"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storagenode/client"
	"github.com/kakao/varlog/pkg/mrc/mrconnector"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/varlogpb"
)

// Log is a log interface with thread-safety. Many goroutines can share the same varlog object.
type Log interface {
	io.Closer

	Append(ctx context.Context, topicID types.TopicID, data [][]byte, opts ...AppendOption) AppendResult

	// AppendTo writes a list of data to the log stream identified by the topicID and
	// logStreamID arguments.
	// This method returns an AppendResult that contains a list of metadata for each log entry
	// and an error if partial failures occur.
	// The length of the metadata list can be less than or equal to the number of data since
	// metadata for failed operations is not included in the metadata list.
	AppendTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data [][]byte, opts ...AppendOption) AppendResult

	Subscribe(ctx context.Context, topicID types.TopicID, begin types.GLSN, end types.GLSN, onNextFunc OnNext, opts ...SubscribeOption) (SubscribeCloser, error)

	SubscribeTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.LLSN, opts ...SubscribeOption) Subscriber

	Trim(ctx context.Context, topicID types.TopicID, until types.GLSN, opts TrimOption) error

	// PeekLogStream returns the log sequence numbers at the first and the
	// last. It fetches the metadata for each replica of a log stream lsid
	// concurrently and takes a result from either appendable or sealed
	// replica. If none of the replicas' statuses is either appendable or
	// sealed, it returns an error.
	PeekLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (first varlogpb.LogSequenceNumber, last varlogpb.LogSequenceNumber, err error)
}

type AppendResult struct {
	Metadata []varlogpb.LogEntryMeta
	Err      error
}

type OnNext func(logEntry varlogpb.LogEntry, err error)

type logImpl struct {
	clusterID         types.ClusterID
	refresher         MetadataRefresher
	lsSelector        LogStreamSelector
	replicasRetriever ReplicasRetriever
	allowlist         Allowlist

	logCLManager *client.Manager[*client.LogClient]
	logger       *zap.Logger
	opts         *options

	runner *runner.Runner

	closed atomic.Bool
}

var _ Log = (*logImpl)(nil)

// Open creates new logs or opens an already created logs.
func Open(ctx context.Context, clusterID types.ClusterID, mrAddrs []string, opts ...Option) (Log, error) {
	logOpts := defaultOptions()
	for _, opt := range opts {
		opt.apply(&logOpts)
	}
	logOpts.logger = logOpts.logger.Named("varlog").With(zap.Any("cid", clusterID))

	v := &logImpl{
		clusterID: clusterID,
		logger:    logOpts.logger,
		opts:      &logOpts,
		runner:    runner.New("varlog", logOpts.logger),
	}

	ctx, cancel := context.WithTimeout(ctx, v.opts.openTimeout)
	defer cancel()

	// mr connector
	connector, err := mrconnector.New(ctx,
		mrconnector.WithClusterID(clusterID),
		mrconnector.WithLogger(v.opts.logger),
		mrconnector.WithConnectTimeout(v.opts.mrConnectorConnTimeout),
		mrconnector.WithRPCTimeout(v.opts.mrConnectorCallTimeout),
		mrconnector.WithSeed(mrAddrs),
	)
	if err != nil {
		return nil, err
	}

	// allowlist
	allowlist, err := newTransientAllowlist(v.opts.denyTTL, v.opts.expireDenyInterval, v.logger)
	if err != nil {
		return nil, err
	}
	v.allowlist = allowlist

	// log stream selector
	v.lsSelector = newAppendableLogStreamSelector(allowlist)

	// replicas retriever
	replicasRetriever := &renewableReplicasRetriever{}
	v.replicasRetriever = replicasRetriever

	// metadata refresher
	// NOTE: metadata refresher has ownership of mrconnector and allow/denylist
	refresher, err := newMetadataRefresher(
		ctx,
		connector,
		allowlist,
		replicasRetriever,
		v.opts.metadataRefreshInterval,
		v.opts.metadataRefreshTimeout,
		v.logger,
	)
	if err != nil {
		return nil, err
	}
	v.refresher = refresher

	// logcl manager
	// TODO (jun): metadataRefresher should implement ClusterMetadataView
	metadata := refresher.Metadata()

	logCLManager, err := client.NewManager[*client.LogClient](
		client.WithDefaultGRPCDialOptions(v.opts.grpcDialOptions...),
		client.WithLogger(v.logger),
	)
	if err != nil {
		return nil, err
	}
	v.logCLManager = logCLManager

	for _, snd := range metadata.GetStorageNodes() {
		if _, err := v.logCLManager.GetOrConnect(ctx, snd.StorageNodeID, snd.Address); err != nil {
			_ = v.logCLManager.Close()
			return nil, err
		}
	}

	return v, nil
}

func (v *logImpl) Append(ctx context.Context, topicID types.TopicID, data [][]byte, opts ...AppendOption) AppendResult {
	return v.append(ctx, topicID, 0, data, opts...)
}

func (v *logImpl) AppendTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data [][]byte, opts ...AppendOption) AppendResult {
	opts = append(opts, withoutSelectLogStream())
	return v.append(ctx, topicID, logStreamID, data, opts...)
}

func (v *logImpl) Subscribe(ctx context.Context, topicID types.TopicID, begin types.GLSN, end types.GLSN, onNextFunc OnNext, opts ...SubscribeOption) (SubscribeCloser, error) {
	return v.subscribe(ctx, topicID, begin, end, onNextFunc, opts...)
}

func (v *logImpl) SubscribeTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.LLSN, opts ...SubscribeOption) Subscriber {
	return v.subscribeTo(ctx, topicID, logStreamID, begin, end, opts...)
}

func (v *logImpl) Trim(ctx context.Context, topicID types.TopicID, until types.GLSN, opts TrimOption) error {
	return v.trim(ctx, topicID, until, opts)
}

func (v *logImpl) PeekLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (first varlogpb.LogSequenceNumber, last varlogpb.LogSequenceNumber, err error) {
	return v.peekLogStream(ctx, tpid, lsid)
}

func (v *logImpl) Close() (err error) {
	if v.closed.Load() {
		return
	}

	if e := v.logCLManager.Close(); e != nil {
		v.logger.Warn("error while closing logcl manager", zap.Error(err))
		err = e
	}
	if e := v.refresher.Close(); e != nil {
		v.logger.Warn("error while closing metadata refresher", zap.Error(err))
		err = e
	}
	v.closed.Store(true)
	return
}
