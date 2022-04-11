package varlogpb

func (snd *StorageNodeDescriptor) Valid() bool {
	if snd == nil ||
		len(snd.Address) == 0 ||
		len(snd.Storages) == 0 {
		return false
	}

	for _, storage := range snd.Storages {
		if !storage.valid() {
			return false
		}
	}

	return true
}
