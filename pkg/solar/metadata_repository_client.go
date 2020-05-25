package solar

type MetadataRepositoryClient interface {
	Propose() error
	Refresh() error
}

type metadataRepositoryClient struct {
	rpcConn    *rpcConn
	projection *Projection
}

func NewMetadataRepositoryClient(address string) (MetadataRepositoryClient, error) {
	rpcConn, err := newRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewMetadataRepositoryClientFromRpcConn(rpcConn)
}

func NewMetadataRepositoryClientFromRpcConn(rpcConn *rpcConn) (MetadataRepositoryClient, error) {
	return &metadataRepositoryClient{
		rpcConn: rpcConn,
	}, nil
}

func (c *metadataRepositoryClient) Close() error {
	return c.rpcConn.close()
}

func (c *metadataRepositoryClient) Propose() error {
	return nil
}

func (c *metadataRepositoryClient) Refresh() error {
	return nil
}
