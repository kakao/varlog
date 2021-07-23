package executor

import (
	"github.com/kakao/varlog/internal/storagenode/id"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
)

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/executor -package executor -destination telemetry_mock.go . MeasurableExecutor

type MeasurableExecutor interface {
	telemetry.Measurable
	id.StorageNodeIDGetter
	id.LogStreamIDGetter
}
