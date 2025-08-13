package rpc

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	// clientKeepaliveInterval defines the time interval for client-side
	// keepalive pings. It is set to 10 seconds since the minimum value
	// supported by gRPC is 10 seconds.
	clientKeepaliveInterval = 10 * time.Second

	// clientKeepaliveTimeout specifies the timeout for client-side keepalive
	// pings. It is set to 1500ms (3*network timeout) to allow sufficient time
	// for transient network issues to resolve.
	clientKeepaliveTimeout = 3 * networkTimeout
)

var defaultDialOption = []grpc.DialOption{
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                clientKeepaliveInterval,
		Timeout:             clientKeepaliveTimeout,
		PermitWithoutStream: true,
	}),
}

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
		return nil, fmt.Errorf("rpc: %s: %w", address, err)
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
			err = c.Conn.Close()
			if err != nil {
				err = fmt.Errorf("rpc: %w", err)
			}
		}
	})
	return err
}
