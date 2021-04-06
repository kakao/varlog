package rpcserver

import (
	"context"

	"google.golang.org/grpc"
)

type Registrable interface {
	Register(server *grpc.Server)
}

func RegisterRPCServer(rpcServer *grpc.Server, servers ...Registrable) {
	for _, server := range servers {
		server.Register(rpcServer)
	}
}

type Handler func(context.Context, interface{}) (interface{}, error)
