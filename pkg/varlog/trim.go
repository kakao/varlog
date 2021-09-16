package varlog

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type TrimOption struct {
}

type trimArgument struct {
	storageNodeID types.StorageNodeID
	address       string
	err           error
}

func (v *varlog) trim(ctx context.Context, topicID types.TopicID, until types.GLSN, opts TrimOption) error {
	trimArgs := createTrimArguments(v.replicasRetriever.All(topicID))
	if len(trimArgs) == 0 {
		return errors.New("no storage node")
	}

	mctx, cancel := v.runner.WithManagedCancel(ctx)
	defer cancel()

	wg := new(sync.WaitGroup)
	wg.Add(len(trimArgs))
	for _, trimArg := range trimArgs {
		trimmer := v.makeTrimmer(trimArg, topicID, until, wg)
		v.runner.RunC(mctx, trimmer)
	}
	wg.Wait()

	for _, trimArg := range trimArgs {
		v.logger.Debug("trim result", zap.Any("snid", trimArg.storageNodeID), zap.Error(trimArg.err))
		if trimArg.err == nil {
			return nil
		}
	}
	return trimArgs[0].err
}

func (v *varlog) makeTrimmer(trimArg *trimArgument, topicID types.TopicID, until types.GLSN, wg *sync.WaitGroup) func(context.Context) {
	return func(ctx context.Context) {
		defer wg.Done()
		logCL, err := v.logCLManager.GetOrConnect(ctx, trimArg.storageNodeID, trimArg.address)
		if err != nil {
			trimArg.err = err
			return
		}
		trimArg.err = logCL.Trim(ctx, topicID, until)
		// TODO (jun): Like subscribe, `ErrUndecidable` is ignored since the local
		// highwatermark of some log streams are less than the `until` of trim.
		// It is a sign of the need to clarify undecidable error in the log stream executor.
		if errors.Is(trimArg.err, verrors.ErrUndecidable) {
			trimArg.err = nil
		}
	}
}

func createTrimArguments(replicasMap map[types.LogStreamID][]varlogpb.LogStreamReplicaDescriptor) []*trimArgument {
	snIDSet := make(map[types.StorageNodeID]struct{})
	var trimArgs []*trimArgument
	for _, replicas := range replicasMap {
		for _, replica := range replicas {
			storageNodeID := replica.GetStorageNodeID()
			if _, ok := snIDSet[storageNodeID]; ok {
				continue
			}
			snIDSet[storageNodeID] = struct{}{}
			storageNodeAddr := replica.GetAddress()
			trimArgs = append(trimArgs, &trimArgument{
				storageNodeID: storageNodeID,
				address:       storageNodeAddr,
			})
		}
	}
	return trimArgs
}
