package rpc

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestNewConn(t *testing.T, ctx context.Context, bufsize ...int) (listener net.Listener, connect func() *Conn) {
	sz := 1 << 10
	if len(bufsize) > 0 {
		sz = bufsize[0]
	}
	lis := bufconn.Listen(sz)
	connect = func() *Conn {
		cc, err := grpc.DialContext(ctx, lis.Addr().String(), grpc.WithContextDialer(
			func(ctx context.Context, addr string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		assert.NoError(t, err)
		return &Conn{Conn: cc}
	}
	return lis, connect
}
