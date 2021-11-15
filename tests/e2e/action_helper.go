package e2e

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/varlogpb"
)

func anySNFail(k8s *K8sVarlogCluster, primary bool) error {
	mrseed, err := k8s.MRAddress()
	if err != nil {
		return err
	}

	connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer connCancel()
	mrcli, err := mrc.NewMetadataRepositoryClient(connCtx, mrseed)
	if err != nil {
		return err
	}
	defer mrcli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	meta, err := mrcli.GetMetadata(ctx)
	if err != nil {
		return err
	}
	lsdescs := meta.GetLogStreams()
	if len(lsdescs) == 0 {
		return errors.New("no logstream")
	}

	idx := rand.Intn(len(lsdescs))
	lsdesc := lsdescs[idx]
	var snID types.StorageNodeID
	if primary {
		snID = lsdesc.GetReplicas()[0].GetStorageNodeID()
	} else {
		snID = lsdesc.Replicas[len(lsdesc.GetReplicas())-1].GetStorageNodeID()
	}
	log.Printf("SNFAIL: snid=%d, lsid=%d\n", snID, lsdesc.GetLogStreamID())
	return k8s.StopSN(snID)
}

func AnyBackupSNFail(k8s *K8sVarlogCluster) func() error {
	return func() error {
		return anySNFail(k8s, false)
	}
}

func AnyPrimarySNFail(k8s *K8sVarlogCluster) func() error {
	return func() error {
		return anySNFail(k8s, true)
	}
}

func WaitSNFail(k8s *K8sVarlogCluster) func() error {
	return func() error {
		var (
			err   error
			n     int
			tries int
		)
		ok := testutil.CompareWaitN(300, func() bool {
			tries++
			n, err = k8s.NumSNRunning()
			ret := err == nil && n < k8s.NrSN
			if !ret {
				defer time.Sleep(100 * time.Millisecond)
			}
			return ret
		})
		if ok {
			return nil
		}
		if err == nil {
			err = errors.New("change check timeout")
		}
		return errors.WithMessagef(err, "tries = %d, n = %d", tries, n)
	}
}

func RecoverSN(k8s *K8sVarlogCluster) func() error {
	return k8s.RecoverSN
}

func RecoverSNCheck(k8s *K8sVarlogCluster) func() error {
	return func() error {
		var (
			err   error
			n     int
			tries int
		)
		ok := testutil.CompareWaitN(300, func() bool {
			tries++
			n, err = k8s.NumSNRunning()
			ret := err == nil && n == k8s.NrSN
			if !ret {
				defer time.Sleep(100 * time.Millisecond)
			}
			return ret
		})
		if ok {
			return nil
		}
		if err == nil {
			err = errors.New("recover check timeout")
		}
		return errors.WithMessagef(err, "tries = %d, n = %d", tries, n)
	}
}

func mrFail(k8s *K8sVarlogCluster, leader bool) error {
	vmsaddr, err := k8s.VMSAddress()
	if err != nil {
		return err
	}

	connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer connCancel()
	mcli, err := varlog.NewAdmin(connCtx, vmsaddr)
	if err != nil {
		return err
	}
	defer mcli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	members, err := mcli.GetMRMembers(ctx)
	if err != nil {
		return err
	}

	if len(members.Members) != k8s.NrMR {
		return errors.New("not enough # of mr")
	}

	for nodeID := range members.Members {
		if leader == (nodeID == members.Leader) {
			return k8s.StopMR(nodeID)
		}
	}

	return errors.New("no target mr")
}

func FollowerMRFail(k8s *K8sVarlogCluster) func() error {
	return func() error {
		return mrFail(k8s, false)
	}
}

func LeaderMRFail(k8s *K8sVarlogCluster) func() error {
	return func() error {
		return mrFail(k8s, true)
	}
}

func WaitMRFail(k8s *K8sVarlogCluster) func() error {
	return func() error {
		var (
			err   error
			n     int
			tries int
		)
		ok := testutil.CompareWaitN(300, func() bool {
			tries++
			n, err := k8s.NumMRRunning()
			ret := err == nil && n < k8s.NrMR
			if !ret {
				defer time.Sleep(100 * time.Millisecond)
			}
			return ret
		})
		if ok {
			return nil
		}
		if err == nil {
			err = errors.New("change check timeout")
		}
		return errors.WithMessagef(err, "tries = %d, n = %d", tries, n)
	}
}

func RecoverMR(k8s *K8sVarlogCluster) func() error {
	return k8s.RecoverMR
}

func RecoverMRCheck(k8s *K8sVarlogCluster) func() error {
	return func() error {
		var (
			err   error
			n     int
			tries int
		)
		ok := testutil.CompareWaitN(300, func() bool {
			tries++
			n, err = k8s.NumMRRunning()
			ret := err == nil && n == k8s.NrMR
			if !ret {
				defer time.Sleep(100 * time.Millisecond)
			}
			return ret
		})
		if ok {
			return nil
		}
		if err == nil {
			err = errors.New("recover check timeout")
		}
		return errors.WithMessagef(err, "tries = %d, n = %d", tries, n)
	}
}

func InitLogStream(k8s *K8sVarlogCluster) func() error {
	return func() error {
		vmsaddr, err := k8s.VMSAddress()
		if err != nil {
			return err
		}

		connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer connCancel()
		mcli, err := varlog.NewAdmin(connCtx, vmsaddr)
		if err != nil {
			return err
		}
		defer mcli.Close()

		addTopicCtx, addTopicCancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer addTopicCancel()
		topic, err := mcli.AddTopic(addTopicCtx)
		if err != nil {
			return err
		}
		topicID := topic.TopicID

		for i := 0; i < k8s.NrLS; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
			_, err = mcli.AddLogStream(ctx, topicID, nil)
			cancel()
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func AddLogStream(k8s *K8sVarlogCluster, topicID types.TopicID) func() error {
	return func() error {
		vmsaddr, err := k8s.VMSAddress()
		if err != nil {
			return err
		}

		connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer connCancel()
		mcli, err := varlog.NewAdmin(connCtx, vmsaddr)
		if err != nil {
			return err
		}
		defer mcli.Close()

		ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
		_, err = mcli.AddLogStream(ctx, topicID, nil)
		defer cancel()

		return err
	}
}

func SealAnyLogStream(k8s *K8sVarlogCluster) func() error {
	return func() error {
		vmsaddr, err := k8s.VMSAddress()
		if err != nil {
			return err
		}

		mrseed, err := k8s.MRAddress()
		if err != nil {
			return err
		}

		connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer connCancel()
		mrcli, err := mrc.NewMetadataRepositoryClient(connCtx, mrseed)
		if err != nil {
			return err
		}
		defer mrcli.Close()

		ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer cancel()
		meta, err := mrcli.GetMetadata(ctx)
		if err != nil {
			return err
		}
		lsdescs := meta.GetLogStreams()
		if len(lsdescs) == 0 {
			return errors.New("no logstream")
		}

		idx := rand.Intn(len(lsdescs))
		for i := 0; i < len(lsdescs); i++ {
			if !lsdescs[idx].Status.Sealed() {
				break
			}

			if i == len(lsdescs)-1 {
				return errors.New("no more running logstream")
			}

			idx = (idx + 1) % len(lsdescs)
		}

		mconnCtx, mconnCancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer mconnCancel()
		mcli, err := varlog.NewAdmin(mconnCtx, vmsaddr)
		if err != nil {
			return err
		}
		defer mcli.Close()

		mctx, mcancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer mcancel()
		_, err = mcli.Seal(mctx, lsdescs[idx].TopicID, lsdescs[idx].LogStreamID)
		if err != nil {
			return err
		}

		return nil
	}
}

func ReconfigureSealedLogStreams(k8s *K8sVarlogCluster) func() error {
	return func() error {
		mrseed, err := k8s.MRAddress()
		if err != nil {
			return err
		}

		connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer connCancel()
		mrcli, err := mrc.NewMetadataRepositoryClient(connCtx, mrseed)
		if err != nil {
			return err
		}
		defer mrcli.Close()

		ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer cancel()
		meta, err := mrcli.GetMetadata(ctx)
		if err != nil {
			return err
		}
		lsdescs := meta.GetLogStreams()
		if len(lsdescs) == 0 {
			return errors.New("no logstream")
		}

		for _, lsdesc := range lsdescs {
			if lsdesc.Status.Sealed() {
				err = updateSealedLogStream(k8s, meta, lsdesc)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}
}

func updateSealedLogStream(k8s *K8sVarlogCluster, meta *varlogpb.MetadataDescriptor, lsdesc *varlogpb.LogStreamDescriptor) error {
	vmsaddr, err := k8s.VMSAddress()
	if err != nil {
		return err
	}

	tpID := lsdesc.TopicID
	lsID := lsdesc.LogStreamID
	pushReplica, popReplica := getPushPopReplicas(k8s, meta, lsdesc.LogStreamID)

	if pushReplica == nil || popReplica == nil {
		return errors.New("no push/pop replicas")
	}

	mconnCtx, mconnCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer mconnCancel()
	mcli, err := varlog.NewAdmin(mconnCtx, vmsaddr)
	if err != nil {
		return err
	}
	defer mcli.Close()

	mctx, mcancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer mcancel()
	_, err = mcli.UpdateLogStream(mctx, tpID, lsID, popReplica, pushReplica)
	if err != nil {
		return err
	}

	log.Printf("UPDATELS: lsid=%d, pop:%+v, push:+%v\n", lsID, popReplica, pushReplica)

	return nil
}

func getPushPopReplicas(k8s *K8sVarlogCluster, meta *varlogpb.MetadataDescriptor, lsID types.LogStreamID) (*varlogpb.ReplicaDescriptor, *varlogpb.ReplicaDescriptor) {
	lsdesc := meta.GetLogStream(lsID)
	if lsdesc == nil {
		return nil, nil
	}

	v, ok := victimReplica(k8s, lsdesc)
	if !ok {
		return nil, nil
	}

	pop := proto.Clone(v).(*varlogpb.ReplicaDescriptor)

	var push *varlogpb.ReplicaDescriptor
	meta = proto.Clone(meta).(*varlogpb.MetadataDescriptor)

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(meta.StorageNodes), func(i, j int) {
		meta.StorageNodes[i], meta.StorageNodes[j] = meta.StorageNodes[j], meta.StorageNodes[i]
	})

	for _, sndesc := range meta.GetStorageNodes() {
		if !lsdesc.IsReplica(sndesc.StorageNodeID) {
			push = &varlogpb.ReplicaDescriptor{
				StorageNodeID: sndesc.StorageNodeID,
				Path:          sndesc.Storages[0].Path,
			}
		}
	}

	return push, pop
}

func victimReplica(k8s *K8sVarlogCluster, lsdesc *varlogpb.LogStreamDescriptor) (*varlogpb.ReplicaDescriptor, bool) {
	for _, r := range lsdesc.Replicas {
		if k8s.StoppedSN(r.StorageNodeID) {
			return r, true
		}
	}

	idx := rand.Intn(len(lsdesc.Replicas))
	return lsdesc.Replicas[idx], false
}

func UnregisterStoppedSN(k8s *K8sVarlogCluster) func() error {
	return func() error {
		mrseed, err := k8s.MRAddress()
		if err != nil {
			return err
		}

		connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer connCancel()
		mrcli, err := mrc.NewMetadataRepositoryClient(connCtx, mrseed)
		if err != nil {
			return err
		}
		defer mrcli.Close()

		ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
		defer cancel()
		meta, err := mrcli.GetMetadata(ctx)
		if err != nil {
			return err
		}

		for _, sndesc := range meta.GetStorageNodes() {
			if k8s.StoppedSN(sndesc.StorageNodeID) {
				if err := unregisterSN(k8s, sndesc.StorageNodeID); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func unregisterSN(k8s *K8sVarlogCluster, snID types.StorageNodeID) error {
	vmsaddr, err := k8s.VMSAddress()
	if err != nil {
		return err
	}

	mconnCtx, mconnCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer mconnCancel()
	mcli, err := varlog.NewAdmin(mconnCtx, vmsaddr)
	if err != nil {
		return err
	}
	defer mcli.Close()

	mctx, mcancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer mcancel()
	if err := mcli.UnregisterStorageNode(mctx, snID); err != nil {
		return err
	}

	log.Printf("UNREGISTERSN: snid:%d\n", snID)

	return nil
}

func WaitSealed(k8s *K8sVarlogCluster) func() error {
	return func() error {
		var (
			err   error
			tries int
		)
		dur := time.Now()
		ok := testutil.CompareWaitN(300, func() bool {
			tries++
			sealed, err := isAllSealed(k8s)
			ret := err == nil && sealed
			if !ret {
				defer time.Sleep(100 * time.Millisecond)
			}
			return ret
		})
		if ok {
			return nil
		}
		if err == nil {
			err = errors.New("change check timeout")
		}
		return errors.WithMessagef(err, "tries = %d, dur = %v", tries, time.Since(dur))
	}
}

func isAllSealed(k8s *K8sVarlogCluster) (bool, error) {
	mrseed, err := k8s.MRAddress()
	if err != nil {
		return false, err
	}

	connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer connCancel()
	mrcli, err := mrc.NewMetadataRepositoryClient(connCtx, mrseed)
	if err != nil {
		return false, err
	}
	defer mrcli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	meta, err := mrcli.GetMetadata(ctx)
	if err != nil {
		return false, err
	}
	lsdescs := meta.GetLogStreams()
	if len(lsdescs) == 0 {
		return true, nil
	}

	for _, lsdesc := range lsdescs {
		if lsdesc.Status.Sealed() {
			sealed, err := isSealed(k8s, meta, lsdesc.LogStreamID)
			if !sealed || err != nil {
				return sealed, err
			}
		}
	}

	return true, nil
}

func isSealed(k8s *K8sVarlogCluster, meta *varlogpb.MetadataDescriptor, lsID types.LogStreamID) (bool, error) {
	lsdesc := meta.GetLogStream(lsID)
	if lsdesc == nil {
		return false, fmt.Errorf("no lsdesc")
	}

	for _, r := range lsdesc.Replicas {
		sndesc := meta.GetStorageNode(r.StorageNodeID)
		if sndesc == nil {
			return false, fmt.Errorf("no sndesc")
		}

		sealed, err := isReplicaSealed(k8s, sndesc.Address, lsID)
		if !sealed || err != nil {
			return sealed, err
		}
	}

	return true, nil
}

func isReplicaSealed(k8s *K8sVarlogCluster, addr string, lsID types.LogStreamID) (bool, error) {
	connCtx, connCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer connCancel()
	sncli, err := snc.NewManagementClient(connCtx, types.ClusterID(1), addr, zap.NewNop())
	if err != nil {
		return false, err
	}
	defer sncli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	meta, err := sncli.GetMetadata(ctx)
	if err != nil {
		return false, err
	}

	lsdesc, ok := meta.GetLogStream(lsID)
	if !ok {
		return false, fmt.Errorf("no sn metadata")
	}

	return lsdesc.Status == varlogpb.LogStreamStatusSealed, nil
}
