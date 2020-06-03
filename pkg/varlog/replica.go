package varlog

type ReplicaSet struct {
	LogStream
	StorageNodeAddresses []string
}
