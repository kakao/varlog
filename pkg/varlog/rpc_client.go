package varlog

import (
	"time"

	"google.golang.org/grpc"
)

const RPC_CONN_TIMEOUT = 100 * time.Millisecond

type RpcConn struct {
	Conn *grpc.ClientConn
}

func NewRpcConn(address string) (*RpcConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(RPC_CONN_TIMEOUT))
	if err != nil {
		return nil, err
	}
	return &RpcConn{Conn: conn}, nil
}

func (c *RpcConn) Close() error {
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}
