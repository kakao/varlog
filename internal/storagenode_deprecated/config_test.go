package storagenode_deprecated

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	testCases := []struct {
		name     string
		opts     []Option
		expected bool
	}{
		{
			name:     "no options",
			opts:     nil,
			expected: false,
		},
		{
			name: "single volume",
			opts: []Option{
				WithVolumes(t.TempDir()),
			},
			expected: true,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			_, err := newConfig(tc.opts)
			if tc.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
