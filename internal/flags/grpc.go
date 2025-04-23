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
	// GRPCServerSharedWriteBuffer is a flag to enable sharing gRPC server's transport write buffer across connections.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#WithSharedWriteBuffer
	//   - https://github.com/grpc/grpc-go/pull/6309
	GRPCServerSharedWriteBuffer = &cli.BoolFlag{
		Name:     "grpc-server-shared-write-buffer",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_SERVER_SHARED_WRITE_BUFFER"},
		Usage:    "Enable sharing gRPC server's transport write buffer across connections. If not set, each connection will allocate its own write buffer.",
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
	// GRPCClientSharedWriteBuffer is a flag to enable sharing gRPC client's transport write buffer across connections.
	//
	// See:
	//   - https://pkg.go.dev/google.golang.org/grpc#WithSharedWriteBuffer
	//   - https://github.com/grpc/grpc-go/pull/6309
	GRPCClientSharedWriteBuffer = &cli.BoolFlag{
		Name:     "grpc-client-shared-write-buffer",
		Category: CategoryGRPC,
		EnvVars:  []string{"GRPC_CLIENT_SHARED_WRITE_BUFFER"},
		Usage:    "Enable sharing gRPC client's transport write buffer across connections. If not set, each connection will allocate its own write buffer.",
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
	opts = append(opts, grpc.SharedWriteBuffer(c.Bool(GRPCServerSharedWriteBuffer.Name)))
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
	opts = append(opts, grpc.WithSharedWriteBuffer(c.Bool(GRPCClientSharedWriteBuffer.Name)))
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
