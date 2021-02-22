package rpc

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var (
	defaultDialOption = []grpc.DialOption{grpc.WithInsecure()}
)

type Conn struct {
	Conn *grpc.ClientConn
	once sync.Once
}

func NewConn(ctx context.Context, address string, opts ...grpc.DialOption) (*Conn, error) {
	dialOpts := append(defaultDialOption, opts...)
	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		return nil, errors.Wrapf(err, "rpc: %s", address)
	}
	return &Conn{Conn: conn}, nil
}

func NewBlockingConn(ctx context.Context, address string) (*Conn, error) {
	return NewConn(ctx, address, grpc.WithBlock(), grpc.WithReturnConnectionError())
}

func (c *Conn) Close() (err error) {
	c.once.Do(func() {
		if c.Conn != nil {
			err = errors.Wrap(c.Conn.Close(), "rpc")
		}
	})
	return err
}
