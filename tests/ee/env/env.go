package env

import (
	"context"
	"testing"
)

type Env interface {
	StartAdminServer(ctx context.Context, t *testing.T)
	StopAdminServer(ctx context.Context, t *testing.T)

	String() string
}
