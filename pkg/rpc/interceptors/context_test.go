package interceptors

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFullMethod(t *testing.T) {
	tcs := []struct {
		fullMethod string
		service    string
		method     string
	}{
		{
			fullMethod: "/varlog.admpb.ClusterManager/ListStorageNodes",
			service:    "varlog.admpb.ClusterManager",
			method:     "ListStorageNodes",
		},
		{
			fullMethod: "varlog.admpb.ClusterManager/OuterMethod/InnerMethod",
			service:    "",
			method:     "",
		},
		{
			fullMethod: "/varlog.admpb.ClusterManager/OuterMethod/InnerMethod",
			service:    "",
			method:     "",
		},
		{
			fullMethod: "varlog.admpb.ClusterManager/ListStorageNodes",
			service:    "",
			method:     "",
		},
		{
			fullMethod: "/varlog.admpb.ClusterManager",
			service:    "",
			method:     "",
		},
		{
			fullMethod: "ListStorageNodes",
			service:    "",
			method:     "",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.fullMethod, func(t *testing.T) {
			service, method := ParseFullMethod(tc.fullMethod)
			require.Equal(t, tc.service, service)
			require.Equal(t, tc.method, method)
		})
	}
}
