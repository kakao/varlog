package varlog

import "google.golang.org/grpc"

type rpcConn struct {
	conn *grpc.ClientConn
}

func newRpcConn(address string) (*rpcConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &rpcConn{conn: conn}, nil
}

func (c *rpcConn) close() error {
	return c.conn.Close()
}
