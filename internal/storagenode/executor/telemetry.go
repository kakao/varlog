package executor

import (
	"github.daumkakao.com/varlog/varlog/internal/storagenode/id"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/telemetry"
)

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/executor -package executor -destination executor_mock.go . MeasurableExecutor

type MeasurableExecutor interface {
	telemetry.Measurable
	id.StorageNodeIDGetter
	id.LogStreamIDGetter
}
