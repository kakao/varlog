package k8s

import (
	"context"
	"testing"

	"github.com/kakao/varlog/tests/ee/env"
)

type k8sEnv struct {
}

var _ env.Env = (*k8sEnv)(nil)

func NewEnv(t *testing.T) env.Env {
	return &k8sEnv{}
}

func (e *k8sEnv) StartAdminServer(ctx context.Context, t *testing.T) {
	panic("not implemented")
}

func (e *k8sEnv) StopAdminServer(ctx context.Context, t *testing.T) {
	panic("not implemented")
}

func (e *k8sEnv) String() string {
	return "k8s"
}
