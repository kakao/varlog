package solar

import (
	"context"

	pb "github.daumkakao.com/solar/solar/proto/storage_node"
)

type StorageNodeClient interface {
	Close() error
	Read(ctx context.Context, epoch uint64, glsn uint64) ([]byte, error)
	Append(ctx context.Context, epoch uint64, glsn uint64, data []byte) error
	Fill(ctx context.Context, epoch uint64, glsn uint64) error
	Trim(ctx context.Context, epoch uint64, glsn uint64) error
	Seal(ctx context.Context, epoch uint64) (uint64, error)
}

type storageNodeClient struct {
	rpcConn *rpcConn

	storageNodeID string
	logStream     LogStream
	client        pb.StorageNodeServiceClient
}

func NewStorageNodeClient(address string) (StorageNodeClient, error) {
	rpcConn, err := newRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewStorageNodeClientFromRpcConn(rpcConn)
}

func NewStorageNodeClientFromRpcConn(rpcConn *rpcConn) (StorageNodeClient, error) {
	client := &storageNodeClient{
		rpcConn: rpcConn,
		client:  pb.NewStorageNodeServiceClient(rpcConn.conn),
	}
	return client, nil
}

func (c *storageNodeClient) Close() error {
	return c.rpcConn.close()
}

func (c *storageNodeClient) Read(ctx context.Context, epoch uint64, glsn uint64) ([]byte, error) {
	req := &pb.StorageNodeRequest{
		Api:   pb.READ,
		Epoch: epoch,
		Glsn:  glsn,
	}
	rsp, err := c.client.Call(ctx, req)
	if err != nil {
		return nil, err
	}
	return rsp.GetData(), nil
}

func (c *storageNodeClient) Append(ctx context.Context, epoch uint64, glsn uint64, data []byte) error {
	req := &pb.StorageNodeRequest{
		Api:   pb.APPEND,
		Epoch: epoch,
		Glsn:  glsn,
		Data:  data,
	}
	_, err := c.client.Call(ctx, req)
	return err
}

func (c *storageNodeClient) Fill(ctx context.Context, epoch uint64, glsn uint64) error {
	req := &pb.StorageNodeRequest{
		Api:   pb.FILL,
		Epoch: epoch,
		Glsn:  glsn,
	}
	_, err := c.client.Call(ctx, req)
	return err
}

func (c *storageNodeClient) Trim(ctx context.Context, epoch uint64, glsn uint64) error {
	req := &pb.StorageNodeRequest{
		Api:   pb.TRIM,
		Epoch: epoch,
		Glsn:  glsn,
	}
	_, err := c.client.Call(ctx, req)
	return err
}

func (c *storageNodeClient) Seal(ctx context.Context, epoch uint64) (uint64, error) {
	panic("unimplemented")
}
