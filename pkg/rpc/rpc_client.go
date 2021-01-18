package rpc

import (
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

func NewConn(address string) (*Conn, error) {
	// TODO (jun): Adding WithBlock changes behavior of NewConn; it wat non-blocking function,
	// but it now blocks.
	// FIXME (jun): Provides options, for example connection timeout, blocking or non-blocking,
	// and etc.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(defaultConnTimeout), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrapf(err, "rpc: %s", address)
	}
	return &Conn{Conn: conn}, nil
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
