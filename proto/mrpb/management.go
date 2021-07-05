package mrpb

func (ci *ClusterInfo) NewerThan(other *ClusterInfo) bool {
	return ci.AppliedIndex > other.AppliedIndex
}
