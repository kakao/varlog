package varlog

import (
	"context"
	"io"

	types "github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
)

// StorageNode is a structure to represent identifier and address of storage node.
type StorageNode struct {
	ID   types.StorageNodeID
	Addr string
}

type LogEntry struct {
	GLSN types.GLSN
	LLSN types.LLSN
	Data []byte
}

type SubscribeResult struct {
	*LogEntry
	Error error
}

// StorageNodeClient contains methods to use basic operations - append, read, subscribe, trim of
// single storage node.
type StorageNodeClient interface {
	Append(ctx context.Context, logStreamID types.LogStreamID, data []byte, backups ...StorageNode) (types.GLSN, error)
	Read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN) (*LogEntry, error)
	Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error)
	Trim(ctx context.Context, glsn types.GLSN) (uint64, error)
	Close() error
}

type storageNodeClient struct {
	rpcConn   *rpcConn
	rpcClient pb.StorageNodeServiceClient
	s         StorageNode
}

func NewStorageNodeClient(address string) (StorageNodeClient, error) {
	rpcConn, err := newRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewStorageNodeClientFromRpcConn(rpcConn)
}

func NewStorageNodeClientFromRpcConn(rpcConn *rpcConn) (StorageNodeClient, error) {
	return &storageNodeClient{
		rpcConn:   rpcConn,
		rpcClient: pb.NewStorageNodeServiceClient(rpcConn.conn),
	}, nil
}

// Append sends given data to the log stream in the storage node. To replicate the data, it
// provides argument backups that indicate backup storage nodes. If append operation completes
// successfully,  valid GLSN is sent to the caller. When it goes wrong, zero is returned.
func (c *storageNodeClient) Append(ctx context.Context, logStreamID types.LogStreamID, data []byte, backups ...StorageNode) (types.GLSN, error) {
	req := &pb.AppendRequest{
		Payload: data,
	}
	for _, b := range backups {
		req.Backups = append(req.Backups, pb.AppendRequest_BackupNode{
			StorageNodeID: b.ID,
			LogStreamID:   logStreamID,
			Address:       b.Addr,
		})
	}
	rsp, err := c.rpcClient.Append(ctx, req)
	if err != nil {
		return 0, err
	}
	return rsp.GetGLSN(), nil
}

// Read operation asks the storage node to retrieve data at a given log position in the log stream.
func (c *storageNodeClient) Read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN) (*LogEntry, error) {
	req := &pb.ReadRequest{
		GLSN: glsn,
	}
	rsp, err := c.rpcClient.Read(ctx, req)
	if err != nil {
		return nil, err
	}
	return &LogEntry{
		GLSN: rsp.GetGLSN(),
		LLSN: rsp.GetLLSN(),
		Data: rsp.GetPayload(),
	}, nil
}

// Subscribe gets log entries continuously from the storage node. It guarantees that LLSNs of log
// entries taken are sequential.
func (c *storageNodeClient) Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error) {
	req := &pb.SubscribeRequest{GLSN: glsn}
	stream, err := c.rpcClient.Subscribe(ctx, req)
	if err != nil {
		return nil, err
	}

	out := make(chan SubscribeResult)
	go func(ctx context.Context) {
		defer close(out)
		first := true
		pllsn := types.LLSN(0)
		for {
			rsp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			result := SubscribeResult{Error: err}
			cllsn := rsp.GetLLSN()
			if !first && pllsn+1 != cllsn {
				// TODO: set out of order stream error to result
				panic("not yet implemented")
			}
			if first {
				first = false
			}
			pllsn = cllsn
			if result.Error == nil {
				result.LogEntry = &LogEntry{
					GLSN: rsp.GetGLSN(),
					LLSN: cllsn,
					Data: rsp.GetPayload(),
				}
			}
			select {
			case out <- result:
			case <-ctx.Done():
			}
		}
	}(ctx)
	return out, nil
}

// Trim deletes log entries greater than or equal to given GLSN in the storage node. The number of
// deleted log entries are returned.
func (c *storageNodeClient) Trim(ctx context.Context, glsn types.GLSN) (uint64, error) {
	req := &pb.TrimRequest{
		GLSN:  glsn,
		Async: false,
	}
	rsp, err := c.rpcClient.Trim(ctx, req)
	if err != nil {
		return 0, err
	}
	return rsp.GetNumTrimmed(), nil
}

// Close closes connection to the storage node.
func (c *storageNodeClient) Close() error {
	return c.rpcConn.close()
}
