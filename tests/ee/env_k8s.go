//go:build k8s

package ee

import (
	"testing"

	"github.com/kakao/varlog/tests/ee/env"
	"github.com/kakao/varlog/tests/ee/k8s"
)

func NewEnv(t *testing.T) env.Env {
	return k8s.NewEnv(t)
}
