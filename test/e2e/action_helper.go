package e2e

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
)

func anySNFail(k8s *K8sVarlogCluster, primary bool) error {
	mrseed, err := k8s.MRAddress()
	if err != nil {
		return err
	}

	mrcli, err := mrc.NewMetadataRepositoryClient(mrseed)
	if err != nil {
		return err
	}
	defer mrcli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), k8s.rpcTimeout)
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
	fmt.Printf("SNFAIL: snid=%d, lsid=%d\n", snID, lsdesc.GetLogStreamID())
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
		ok := testutil.CompareWaitN(200, func() bool {
			n, err := k8s.NumSNRunning()
			if err != nil {
				return false
			}
			return n < k8s.NrSN
		})

		if ok {
			return nil
		}

		return errors.New("change check timeout")
	}
}

func RecoverSN(k8s *K8sVarlogCluster) func() error {
	return k8s.RecoverSN
}

func RecoverSNCheck(k8s *K8sVarlogCluster) func() error {
	return func() error {
		ok := testutil.CompareWaitN(100, func() bool {
			n, err := k8s.NumSNRunning()
			if err != nil {
				return false
			}
			return n == k8s.NrSN
		})

		if ok {
			return nil
		}

		return errors.New("recover check timeout")
	}
}

func mrFail(k8s *K8sVarlogCluster, leader bool) error {
	vmsaddr, err := k8s.VMSAddress()
	if err != nil {
		return err
	}

	mcli, err := varlog.NewClusterManagerClient(vmsaddr)
	if err != nil {
		return err
	}
	defer mcli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), k8s.rpcTimeout)
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
		ok := testutil.CompareWaitN(100, func() bool {
			n, err := k8s.NumMRRunning()
			if err != nil {
				return false
			}
			return n < k8s.NrMR
		})

		if ok {
			return nil
		}

		return errors.New("chage check timeout")
	}
}

func RecoverMR(k8s *K8sVarlogCluster) func() error {
	return k8s.RecoverMR
}

func RecoverMRCheck(k8s *K8sVarlogCluster) func() error {
	return func() error {
		ok := testutil.CompareWaitN(100, func() bool {
			n, err := k8s.NumMRRunning()
			if err != nil {
				return false
			}
			return n == k8s.NrMR
		})

		if ok {
			return nil
		}

		return errors.New("recover check timeout")
	}
}

func AddLogStream(k8s *K8sVarlogCluster) func() error {
	return func() error {
		vmsaddr, err := k8s.VMSAddress()
		if err != nil {
			return err
		}

		mcli, err := varlog.NewClusterManagerClient(vmsaddr)
		if err != nil {
			return err
		}
		defer mcli.Close()

		for i := 0; i < k8s.NrLS; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), k8s.rpcTimeout)
			_, err = mcli.AddLogStream(ctx, nil)
			cancel()
			if err != nil {
				return err
			}
		}

		return nil
	}
}
