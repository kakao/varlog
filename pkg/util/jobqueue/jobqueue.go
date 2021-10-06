package jobqueue

import (
	"context"
)

type JobQueue interface {
	PushWithContext(ctx context.Context, item interface{}) error

	PopWithContext(ctx context.Context) (interface{}, error)

	Pop() interface{}

	Size() int
}
