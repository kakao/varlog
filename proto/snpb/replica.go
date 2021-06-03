package snpb

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
		if x.StorageNodeID != y.StorageNodeID || x.LogStreamID != y.LogStreamID {
			return false
		}
	}
	return true
}
