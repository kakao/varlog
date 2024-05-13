package flags

import (
	"fmt"
	"math"

	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/util/units"
)

const (
	CategoryGRPC = "gRPC:"
)

var (
	// GRPCServerReadBufferSize is a flag to set the gRPC server's read buffer
	// size for a single read syscall.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#ReadBufferSize
	GRPCServerReadBufferSize = &cli.StringFlag{
		Name:     "grpc-server-read-buffer-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_SERVER_READ_BUFFER_SIZE"},
		Usage:    "Set the gRPC server's read buffer size for a single read syscall. If not set, the default value of 32KiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-server-read-buffer-size", value)
			}
			return nil
		},
	}
	// GRPCServerWriteBufferSize is a flag to set the gRPC server's write
	// buffer size for a single write syscall.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#WriteBufferSize
	GRPCServerWriteBufferSize = &cli.StringFlag{
		Name:     "grpc-server-write-buffer-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_SERVER_WRITE_BUFFER_SIZE"},
		Usage:    "Set the gRPC server's write buffer size for a single write syscall. If not set, the default value of 32KiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-server-write-buffer-size", value)
			}
			return nil
		},
	}
	// GRPCServerMaxRecvMsgSize is a flag to set the maximum message size the server can receive.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#MaxRecvMsgSize
	GRPCServerMaxRecvMsgSize = &cli.StringFlag{
		Name:     "grpc-server-max-recv-msg-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_SERVER_MAX_RECV_MSG_SIZE"},
		Usage:    "Set the maximum message size in bytes that the gRPC server can receive. If not set, the default value of 4MiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-server-max-recv-msg-size", value)
			}
			return nil
		},
	}
	// GRPCServerInitialConnWindowSize is a flag to set the gRPC server's initial window size for a connection.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#InitialConnWindowSize
	GRPCServerInitialConnWindowSize = &cli.StringFlag{
		Name:     "grpc-server-initial-conn-window-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_SERVER_INITIAL_CONN_WINDOW_SIZE"},
		Usage:    "Set the gRPC server's initial window size for a connection. If not set, the default value of 64KiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value, 0, math.MaxInt32); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-server-initial-conn-window-size", value)
			}
			return nil
		},
	}
	// GRPCServerInitialWindowSize is a flag to set the gRPC server's initial window size for a stream.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#InitialWindowSize
	GRPCServerInitialWindowSize = &cli.StringFlag{
		Name:     "grpc-server-initial-window-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_SERVER_INITIAL_WINDOW_SIZE"},
		Usage:    "Set the gRPC server's initial window size for a stream. If not set, the default value of 64KiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value, 0, math.MaxInt32); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-server-initial-window-size", value)
			}
			return nil
		},
	}
	// GRPCClientReadBufferSize is a flag to set the gRPC client's read buffer
	// size for a single read syscall.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#WithReadBufferSize
	GRPCClientReadBufferSize = &cli.StringFlag{
		Name:     "grpc-client-read-buffer-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_CLIENT_READ_BUFFER_SIZE"},
		Usage:    "Set the gRPC client's read buffer size for a single read syscall. If not set, the default value of 32KiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-client-read-buffer-size", value)
			}
			return nil
		},
	}
	// GRPCClientWriteBufferSize is a flag to set the gRPC client's write
	// buffer size for a single write syscall.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#WithWriteBufferSize
	GRPCClientWriteBufferSize = &cli.StringFlag{
		Name:     "grpc-client-write-buffer-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_CLIENT_WRITE_BUFFER_SIZE"},
		Usage:    "Set the gRPC client's write buffer size for a single write syscall. If not set, the default value of 32KiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-client-read-buffer-size", value)
			}
			return nil
		},
	}
	// GRPCClientInitialConnWindowSize is a flag to set the gRPC client's initial window size for a connection.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#WithInitialConnWindowSize
	GRPCClientInitialConnWindowSize = &cli.StringFlag{
		Name:     "grpc-client-initial-conn-window-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_CLIENT_INITIAL_CONN_WINDOW_SIZE"},
		Usage:    "Set the gRPC client's initial window size for a connection. If not set, the default value of 64KiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value, 0, math.MaxInt32); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-client-initial-conn-window-size", value)
			}
			return nil
		},
	}
	// GRPCClientInitialWindowSize is a flag to set the gRPC client's initial window size for a stream.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#WithInitialWindowSize
	GRPCClientInitialWindowSize = &cli.StringFlag{
		Name:     "grpc-client-initial-window-size",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_CLIENT_INITIAL_WINDOW_SIZE"},
		Usage:    "Set the gRPC client's initial window size for a stream. If not set, the default value of 64KiB defined by gRPC will be used.",
		Action: func(c *cli.Context, value string) error {
			if _, err := units.FromByteSizeString(value, 0, math.MaxInt32); err != nil {
				return fmt.Errorf("invalid value \"%s\" for flag --grpc-client-initial-window-size", value)
			}
			return nil
		},
	}
)

func ParseGRPCServerOptionFlags(c *cli.Context) (opts []grpc.ServerOption, _ error) {
	if c.IsSet(GRPCServerReadBufferSize.Name) {
		readBufferSize, err := units.FromByteSizeString(c.String(GRPCServerReadBufferSize.Name))
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.ReadBufferSize(int(readBufferSize)))
	}
	if c.IsSet(GRPCServerWriteBufferSize.Name) {
		writeBufferSize, err := units.FromByteSizeString(c.String(GRPCServerWriteBufferSize.Name))
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WriteBufferSize(int(writeBufferSize)))
	}
	if c.IsSet(GRPCServerMaxRecvMsgSize.Name) {
		maxRecvMsgSize, err := units.FromByteSizeString(c.String(GRPCServerMaxRecvMsgSize.Name))
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.MaxRecvMsgSize(int(maxRecvMsgSize)))
	}
	if c.IsSet(GRPCServerInitialConnWindowSize.Name) {
		initialConnWindowSize, err := units.FromByteSizeString(c.String(GRPCServerInitialConnWindowSize.Name), 0, math.MaxInt32)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.InitialConnWindowSize(int32(initialConnWindowSize)))
	}
	if c.IsSet(GRPCServerInitialWindowSize.Name) {
		initialWindowSize, err := units.FromByteSizeString(c.String(GRPCServerInitialWindowSize.Name), 0, math.MaxInt32)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.InitialWindowSize(int32(initialWindowSize)))
	}
	return opts, nil
}

func ParseGRPCDialOptionFlags(c *cli.Context) (opts []grpc.DialOption, err error) {
	if c.IsSet(GRPCClientReadBufferSize.Name) {
		readBufferSize, err := units.FromByteSizeString(c.String(GRPCClientReadBufferSize.Name))
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithReadBufferSize(int(readBufferSize)))
	}
	if c.IsSet(GRPCClientWriteBufferSize.Name) {
		writeBufferSize, err := units.FromByteSizeString(c.String(GRPCClientWriteBufferSize.Name))
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithWriteBufferSize(int(writeBufferSize)))
	}
	if c.IsSet(GRPCClientInitialConnWindowSize.Name) {
		initialConnWindowSize, err := units.FromByteSizeString(c.String(GRPCClientInitialConnWindowSize.Name), 0, math.MaxInt32)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithInitialConnWindowSize(int32(initialConnWindowSize)))
	}
	if c.IsSet(GRPCClientInitialWindowSize.Name) {
		initialWindowSize, err := units.FromByteSizeString(c.String(GRPCClientInitialWindowSize.Name), 0, math.MaxInt32)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithInitialWindowSize(int32(initialWindowSize)))
	}
	return opts, nil
}
