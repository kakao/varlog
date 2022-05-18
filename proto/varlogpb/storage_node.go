package varlogpb

func (snd *StorageNodeDescriptor) Valid() bool {
	if snd == nil || len(snd.Address) == 0 || len(snd.Paths) == 0 {
		return false
	}

	for _, path := range snd.Paths {
		if len(path) == 0 {
			return false
		}
	}

	return true
}
