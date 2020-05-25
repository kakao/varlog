package solar

type LogStream struct {
	minLsn uint64
	maxLsn uint64

	storageNode *StorageNode
}
