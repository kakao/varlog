package solar

type StorageNode struct {
	nodeID     int32
	logStreams []*LogStream
	client     StorageNodeClient
}
