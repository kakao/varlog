package netutil

import (
	"net"
	"testing"
)

func TestGetListenerAddress(t *testing.T) {
	tests := []struct {
		in   string
		out1 string
		out2 string
	}{
		{in: ":8000", out1: "[::]:8000", out2: "127.0.0.1:8000"},
		{in: "0.0.0.0:8000", out1: "[::]:8000", out2: "127.0.0.1:8000"},
		{in: "127.0.0.1:8000", out1: "127.0.0.1:8000", out2: "127.0.0.1:8000"},
		{in: "211.211.56.120:8000", out1: "", out2: ""},
	}
	for i := range tests {
		tt := tests[i]
		t.Run(tt.in, func(t *testing.T) {
			lis, err := net.Listen("tcp", tt.in)
			if err != nil {
				t.Skipf("skip listener error: %v", err)
			}
			defer lis.Close()

			actual, err := GetListenerAddr(lis)
			if err != nil {
				t.Error(err)
			}
			if tt.out1 != actual {
				t.Errorf("%s expected=%s actual=%s", tt.in, tt.out1, actual)
			}

			actual, err = GetListenerLocalAddr(lis)
			if err != nil {
				t.Error(err)
			}
			if tt.out2 != actual {
				t.Errorf("%s expected=%s actual=%s", tt.in, tt.out2, actual)
			}
		})
	}
}
