package solar

/*
// Projection is information about cluster layout.
type Projection struct {
	Epoch uint64

	// sequencer
	SequencerAddress string

	// replica set
	ReplicaSets []*ReplicaSet
}

func NewProjectionFromProto(projection *pb.Projection) *Projection {
	replicaSets := []*ReplicaSet{}
	for _, pbRs := range projection.GetReplicaSets() {
		replicaSet := &ReplicaSet{
			LogStream: LogStream{
				MinLsn: pbRs.GetLogStream().GetMinLsn(),
				MaxLsn: pbRs.GetLogStream().GetMaxLsn(),
			},
			StorageNodeAddresses: pbRs.GetStorageNodeAddresses(),
		}

		replicaSets = append(replicaSets, replicaSet)
	}
	return &Projection{
		Epoch:            projection.GetEpoch(),
		SequencerAddress: projection.GetSequencerAddress(),
		ReplicaSets:      replicaSets,
	}
}

func (p *Projection) GetAllStorageNodeAddresses() []string {
	addrs := []string{}
	m := make(map[string]struct{})
	for _, replicaSet := range p.ReplicaSets {
		for _, snAddr := range replicaSet.StorageNodeAddresses {
			if _, ok := m[snAddr]; ok {
				continue
			}
			m[snAddr] = struct{}{}
			addrs = append(addrs, snAddr)
		}
	}
	return addrs
}

func (p *Projection) GetReplicaSet(glsn uint64) *ReplicaSet {
	return nil
}

func (p *Projection) GetAllStorageNodes() []*StorageNode {
	return nil
}

func (p *Projection) Copy() *Projection {
	return nil
}

func (p *Projection) GetSequencer() *Sequencer {
	return nil
}
*/
