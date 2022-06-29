//go:build !k8s

package ee

import (
	"testing"

	"github.com/kakao/varlog/tests/ee/env"
	"github.com/kakao/varlog/tests/ee/local"
)

func NewEnv(t *testing.T) env.Env {
	return local.NewEnv(t)
}
