package replication

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/replication -package replication -destination connector_mock.go . Connector

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type Connector interface {
	io.Closer
	Get(ctx context.Context, replica varlogpb.LogStreamReplica) (Client, error)
}

type connector struct {
	connectorConfig
	clients map[types.StorageNodeID]*client
	closed  bool
	mu      sync.Mutex
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

func (c *connector) Get(ctx context.Context, replica varlogpb.LogStreamReplica) (Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.WithStack(verrors.ErrClosed)
	}

	cl, ok := c.clients[replica.StorageNode.StorageNodeID]
	if ok {
		return cl, nil
	}
	cl, err := c.newClient(ctx, replica)
	if err != nil {
		return nil, err
	}
	c.clients[replica.StorageNode.StorageNodeID] = cl
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

func (c *connector) newClient(ctx context.Context, replica varlogpb.LogStreamReplica) (*client, error) {
	cl, err := newClient(ctx, append(c.clientOptions, WithReplica(replica))...)
	if err != nil {
		return nil, err
	}
	cl.connector = c
	return cl, nil
}

func (c *connector) delClient(client *client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.clients, client.replica.StorageNode.StorageNodeID)
}
