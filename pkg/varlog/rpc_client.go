package varlog

import "google.golang.org/grpc"

type RpcConn struct {
	Conn *grpc.ClientConn
}

func NewRpcConn(address string) (*RpcConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &RpcConn{Conn: conn}, nil
}

func (c *RpcConn) Close() error {
	return c.Conn.Close()
}
