package replication

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/replication -package replication -destination connector_mock.go . Connector

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

type Connector interface {
	io.Closer
	Get(ctx context.Context, replica snpb.Replica) (Client, error)
}

type connector struct {
	connectorConfig
	clients map[types.StorageNodeID]*client
	closed  bool
	mu      sync.Mutex
	logger  *zap.Logger
}

func NewConnector(opts ...ConnectorOption) (Connector, error) {
	cfg, err := newConnectorConfig(opts)
	if err != nil {
		return nil, err
	}
	c := &connector{
		connectorConfig: *cfg,
		clients:         make(map[types.StorageNodeID]*client),
	}
	return c, nil
}

func (c *connector) Get(ctx context.Context, replica snpb.Replica) (Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.WithStack(verrors.ErrClosed)
	}

	cl, ok := c.clients[replica.StorageNodeID]
	if ok {
		return cl, nil
	}
	cl, err := c.newClient(ctx, replica)
	if err != nil {
		return nil, err
	}
	c.clients[replica.StorageNodeID] = cl
	return cl, nil
}

func (c *connector) Close() (err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	cls := make([]*client, 0, len(c.clients))
	for _, cl := range c.clients {
		cls = append(cls, cl)
	}
	c.mu.Unlock()

	for i := range cls {
		err = multierr.Append(err, cls[i].Close())
	}
	return err
}

func (c *connector) newClient(ctx context.Context, replica snpb.Replica) (*client, error) {
	opts := append(c.clientOptions, WithReplica(replica))
	cl, err := newClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	cl.connector = c
	return cl, nil
}

func (c *connector) delClient(client *client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.clients, client.replica.GetStorageNodeID())
}
