package rpc

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const defaultConnTimeout = 3000 * time.Millisecond

type Conn struct {
	Conn *grpc.ClientConn
	once sync.Once
}

func NewConn(ctx context.Context, address string, opts ...grpc.DialOption) (*Conn, error) {
	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "rpc: %s", address)
	}
	return &Conn{Conn: conn}, nil
}

func NewBlockingConnWithContext(ctx context.Context, address string) (*Conn, error) {
	return NewConn(ctx, address, grpc.WithInsecure(), grpc.WithReturnConnectionError())
}

func NewBlockingConn(address string) (*Conn, error) {
	// TODO (jun): Adding WithBlock changes behavior of NewConn; it was non-blocking function,
	// but it now blocks.
	// FIXME (jun): Provides options, for example connection timeout, blocking or non-blocking,
	// and etc.
	ctx, cancel := context.WithTimeout(context.Background(), defaultConnTimeout)
	defer cancel()
	return NewBlockingConnWithContext(ctx, address)
}

func (c *Conn) Close() (err error) {
	if c.Conn != nil {
		c.once.Do(func() {
			err = errors.Wrap(c.Conn.Close(), "rpc")
			c.Conn = nil
		})
	}
	return err
}
