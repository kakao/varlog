//go:build !k8s

package ee

import (
	"testing"

	"github.daumkakao.com/varlog/varlog/tests/ee/env"
	"github.daumkakao.com/varlog/varlog/tests/ee/local"
)

func NewEnv(t *testing.T) env.Env {
	return local.NewEnv(t)
}
