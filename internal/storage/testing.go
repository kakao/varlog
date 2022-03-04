package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStorage(tb testing.TB, opts ...Option) *Storage {
	defaultOpts := []Option{WithPath(tb.TempDir())}
	s, err := New(append(defaultOpts, opts...)...)
	assert.NoError(tb, err)
	return s
}
