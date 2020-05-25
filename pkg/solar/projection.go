package solar

type Projection struct {
	storageNodeLayout []*ReplicaSet
}

func (p *Projection) GetReplicaSet(glsn uint64) *ReplicaSet {
	return nil
}

func (p *Projection) GetAllStorageNodes() []*StorageNode {
	return nil
}

/*
func (p *Projection) GetSequencer() *Sequencer {
	return nil
}
*/
