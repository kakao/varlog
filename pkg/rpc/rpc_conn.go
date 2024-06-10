package rpc

import (
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var defaultDialOption = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

type Conn struct {
	Conn *grpc.ClientConn
	once sync.Once
}

// NewConn creates a new gRPC connection to the specified address using
// google.golang.org/grpc.NewClient. Note that this method does not perform any
// I/O, meaning the connection is not immediately established. If the server is
// unavailable, this method will not return an error. The connection is
// established automatically when an RPC is made using the returned Conn.
// DialOptions such as WithBlock, WithTimeout, and WithReturnConnectionError
// are not considered.
// Ensure to close the returned Conn after usage to avoid resource leaks.
func NewConn(ctx context.Context, address string, opts ...grpc.DialOption) (*Conn, error) {
	conn, err := grpc.NewClient(address, slices.Concat(defaultDialOption, opts)...)
	if err != nil {
		return nil, errors.Wrapf(err, "rpc: %s", address)
	}
	return &Conn{Conn: conn}, nil
}

// Close terminates the gRPC connection. This method ensures that the
// connection is closed only once, even if called multiple times.
// It is important to call Close after the connection is no longer needed to
// release resources properly.
func (c *Conn) Close() (err error) {
	c.once.Do(func() {
		if c.Conn != nil {
			err = errors.Wrap(c.Conn.Close(), "rpc")
		}
	})
	return err
}
