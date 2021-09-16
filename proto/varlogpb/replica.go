package varlogpb

import (
	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/verrors"
)

func EqualReplicas(xs []Replica, ys []Replica) bool {
	if len(xs) != len(ys) {
		return false
	}

	for idx := range xs {
		x := &xs[idx]
		y := &ys[idx]
		// NOTE: To skip comparison of address, Equal method is not used.
		/*
			if !x.Equal(y) {
				return false
			}
		*/
		if x.StorageNode.StorageNodeID != y.StorageNode.StorageNodeID || x.LogStreamID != y.LogStreamID {
			return false
		}
	}
	return true
}

// ValidReplicas checks whether given replicas are valid. Valid replicas should contain at least one
// replica, and all replicas have the same LogStreamID. They also have different StorageNodeIDs.
func ValidReplicas(replicas []Replica) error {
	if len(replicas) < 1 {
		return errors.Wrap(verrors.ErrInvalid, "no replica")
	}

	lsidSet := set.New(len(replicas))
	snidSet := set.New(len(replicas))
	for _, replica := range replicas {
		lsidSet.Add(replica.LogStreamID)
		snidSet.Add(replica.StorageNode.StorageNodeID)
	}
	if lsidSet.Size() != 1 {
		return errors.Wrap(verrors.ErrInvalid, "LogStreamID mismatch")
	}
	if snidSet.Size() != len(replicas) {
		return errors.Wrap(verrors.ErrInvalid, "StorageNodeID duplicated")
	}
	return nil
}
