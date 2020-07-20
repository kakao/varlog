package varlog

import (
	"context"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
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

// LogIOClient contains methods to use basic operations - append, read, subscribe, trim of
// single storage node.
type LogIOClient interface {
	Append(ctx context.Context, logStreamID types.LogStreamID, data []byte, backups ...StorageNode) (types.GLSN, error)
	Read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN) (*LogEntry, error)
	Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error)
	Trim(ctx context.Context, glsn types.GLSN, async bool) (uint64, error)
	Close() error
}

type logIOClient struct {
	rpcConn   *RpcConn
	rpcClient pb.LogIOClient
	s         StorageNode
}

func NewLogIOClient(address string) (LogIOClient, error) {
	rpcConn, err := NewRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewLogIOClientFromRpcConn(rpcConn)
}

func NewLogIOClientFromRpcConn(rpcConn *RpcConn) (LogIOClient, error) {
	return &logIOClient{
		rpcConn:   rpcConn,
		rpcClient: pb.NewLogIOClient(rpcConn.Conn),
	}, nil
}

// Append sends given data to the log stream in the storage node. To replicate the data, it
// provides argument backups that indicate backup storage nodes. If append operation completes
// successfully,  valid GLSN is sent to the caller. When it goes wrong, zero is returned.
func (c *logIOClient) Append(ctx context.Context, logStreamID types.LogStreamID, data []byte, backups ...StorageNode) (types.GLSN, error) {
	req := &pb.AppendRequest{
		Payload:     data,
		LogStreamID: logStreamID,
	}
	for _, b := range backups {
		req.Backups = append(req.Backups, pb.AppendRequest_BackupNode{
			StorageNodeID: b.ID,
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
func (c *logIOClient) Read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN) (*LogEntry, error) {
	req := &pb.ReadRequest{
		GLSN:        glsn,
		LogStreamID: logStreamID,
	}
	rsp, err := c.rpcClient.Read(ctx, req)
	if err != nil {
		return nil, err
	}
	return &LogEntry{
		GLSN: rsp.GetGLSN(),
		Data: rsp.GetPayload(),
	}, nil
}

// Subscribe gets log entries continuously from the storage node. It guarantees that LLSNs of log
// entries taken are sequential.
func (c *logIOClient) Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error) {
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
			if err == nil && !first && pllsn+1 != rsp.GetLLSN() {
				err = ErrUnordered
			}
			result := SubscribeResult{Error: err}
			if err == nil {
				result.LogEntry = &LogEntry{
					GLSN: rsp.GetGLSN(),
					LLSN: rsp.GetLLSN(),
					Data: rsp.GetPayload(),
				}
			}
			if first {
				first = false
			}
			pllsn = rsp.GetLLSN()
			select {
			case out <- result:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
	return out, nil
}

// Trim deletes log entries greater than or equal to given GLSN in the storage node. The number of
// deleted log entries are returned.
func (c *logIOClient) Trim(ctx context.Context, glsn types.GLSN, async bool) (uint64, error) {
	req := &pb.TrimRequest{
		GLSN:  glsn,
		Async: async,
	}
	rsp, err := c.rpcClient.Trim(ctx, req)
	if err != nil {
		return 0, err
	}
	return rsp.GetNumTrimmed(), nil
}

// Close closes connection to the storage node.
func (c *logIOClient) Close() error {
	return c.rpcConn.Close()
}
