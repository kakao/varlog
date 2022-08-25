package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLintError(t *testing.T) {
	var cond bool
	if cond {
		// lint error
	}
	assert.False(t, cond)

}
