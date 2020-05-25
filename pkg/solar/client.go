package solar

type SolarOption struct {
	sequencerAdddress string
}

type Client struct {
	sequencerClient SequencerClient
	epoch           uint64

	metaReposClient MetadataRepositoryClient
}

/*
func NewClient() (*Client, error) {
	var err error
	client := &Client{epoch: 0}
	client.sequencerClient, err = NewSequencerClient("localhost:9091")
	if err != nil {
		return nil, err
	}
	storageNodeClient, err := NewStorageNodeClient("localhost:9092")
	if err != nil {
		return nil, err
	}
	client.metaReposClient, err = NewDummyMetadataRepositoryClient(storageNodeClient)
	return client, nil
}

func (c *Client) Read(glsn uint64) ([]byte, error) {
	projection, _ := c.metaReposClient.GetProjection()
	storageNodeClient := projection.GetStorageNode(glsn)
	data, err := storageNodeClient.Read(context.Background(), c.epoch, glsn)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Client) Append(data []byte) (uint64, error) {
	glsn, err := c.sequencerClient.Next(context.Background())
	if err != nil {
		return 0, err
	}
	projection, _ := c.metaReposClient.GetProjection()
	storageNodeClient := projection.GetStorageNode(glsn)
	if err := storageNodeClient.Append(context.Background(), c.epoch, glsn, data); err != nil {
		return 0, err
	}
	return glsn, nil
}

func (c *Client) Fill(glsn uint64) error {
	projection, _ := c.metaReposClient.GetProjection()
	storageNodeClient := projection.GetStorageNode(glsn)
	return storageNodeClient.Fill(context.Background(), c.epoch, glsn)
}

func (c *Client) Trim(glsn uint64) error {
	projection, _ := c.metaReposClient.GetProjection()
	storageNodeClient := projection.GetStorageNode(glsn)
	return storageNodeClient.Trim(context.Background(), c.epoch, glsn)
}

func (c *Client) Seal(epoch int64, maxLsn *uint64) error {
	return nil
}
*/
