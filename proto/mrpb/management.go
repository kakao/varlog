package mrpb

import "github.com/kakao/varlog/pkg/types"

func (ci *ClusterInfo) NewerThan(other *ClusterInfo) bool {
	if ci == nil || other == nil {
		return false
	}
	if ci.AppliedIndex > other.AppliedIndex {
		return true
	} else if ci.AppliedIndex < other.AppliedIndex {
		return false
	}

	// NOTE: Some endpoints are filled late, but applied indices of ClusterInfos are same.
	thisEndpoints := ci.definedEndpoints()
	thatEndpoints := other.definedEndpoints()
	return len(thisEndpoints) > len(thatEndpoints)
}

func (ci *ClusterInfo) definedEndpoints() []string {
	endpoints := make([]string, 0, len(ci.GetMembers()))
	for _, member := range ci.GetMembers() {
		if len(member.GetEndpoint()) > 0 {
			endpoints = append(endpoints, member.GetEndpoint())
		}
	}
	return endpoints
}

func (ci *ClusterInfo) ForEachMember(f func(types.NodeID, *ClusterInfo_Member) bool) {
	for nodeID, member := range ci.GetMembers() {
		if !f(nodeID, member) {
			return
		}
	}
}
