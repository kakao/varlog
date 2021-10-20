package varlog

//go:generate mockgen -package varlog -destination varlog_mock.go . Varlog

import (
	"context"
	"io"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/logc"
	"github.daumkakao.com/varlog/varlog/pkg/mrc/mrconnector"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// Varlog is a log interface with thread-safety. Many goroutines can share the same varlog object.
type Varlog interface {
	io.Closer

	Append(ctx context.Context, topicID types.TopicID, data []byte, opts ...AppendOption) (types.GLSN, error)

	AppendTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data []byte, opts ...AppendOption) (types.GLSN, error)

	Read(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, glsn types.GLSN) ([]byte, error)

	Subscribe(ctx context.Context, topicID types.TopicID, begin types.GLSN, end types.GLSN, onNextFunc OnNext, opts ...SubscribeOption) (SubscribeCloser, error)

	Trim(ctx context.Context, topicID types.TopicID, until types.GLSN, opts TrimOption) error
}

type OnNext func(logEntry varlogpb.LogEntry, err error)

type varlog struct {
	clusterID         types.ClusterID
	refresher         MetadataRefresher
	lsSelector        LogStreamSelector
	replicasRetriever ReplicasRetriever
	allowlist         Allowlist

	logCLManager logc.LogClientManager
	logger       *zap.Logger
	opts         *options

	runner *runner.Runner

	closed atomicutil.AtomicBool
}

var _ Varlog = (*varlog)(nil)

// Open creates new logs or opens an already created logs.
func Open(ctx context.Context, clusterID types.ClusterID, mrAddrs []string, opts ...Option) (Varlog, error) {
	logOpts := defaultOptions()
	for _, opt := range opts {
		opt.apply(&logOpts)
	}
	logOpts.logger = logOpts.logger.Named("varlog").With(zap.Any("cid", clusterID))

	v := &varlog{
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

	logCLManager, err := logc.NewLogClientManager(ctx, metadata, v.logger)
	if err != nil {
		return nil, err
	}
	v.logCLManager = logCLManager

	return v, nil
}

func (v *varlog) Append(ctx context.Context, topicID types.TopicID, data []byte, opts ...AppendOption) (types.GLSN, error) {
	return v.append(ctx, topicID, 0, data, opts...)
}

func (v *varlog) AppendTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data []byte, opts ...AppendOption) (types.GLSN, error) {
	opts = append(opts, withoutSelectLogStream())
	return v.append(ctx, topicID, logStreamID, data, opts...)
}

func (v *varlog) Read(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, glsn types.GLSN) ([]byte, error) {
	logEntry, err := v.read(ctx, topicID, logStreamID, glsn)
	if err != nil {
		return nil, err
	}
	return logEntry.Data, nil
}

func (v *varlog) Subscribe(ctx context.Context, topicID types.TopicID, begin types.GLSN, end types.GLSN, onNextFunc OnNext, opts ...SubscribeOption) (SubscribeCloser, error) {
	return v.subscribe(ctx, topicID, begin, end, onNextFunc, opts...)
}

func (v *varlog) Trim(ctx context.Context, topicID types.TopicID, until types.GLSN, opts TrimOption) error {
	return v.trim(ctx, topicID, until, opts)
}

func (v *varlog) Close() (err error) {
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
