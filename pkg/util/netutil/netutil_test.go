package netutil

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/types"
)

func TestGetListenerAddr(t *testing.T) {
	tests := []struct {
		in        string
		out       string
		minOutLen int
	}{
		{in: ":8000", minOutLen: 1},
		{in: "0.0.0.0:8000", minOutLen: 1},
		{in: "127.0.0.1:0", minOutLen: 1},
		{in: "127.0.0.1:8000", out: "127.0.0.1:8000"},
	}
	ips, err := IPs()
	if err != nil {
		t.Fatal(err)
	}
	for _, ip := range ips {
		tests = append(tests, struct {
			in        string
			out       string
			minOutLen int
		}{
			in:  ip.String() + ":8000",
			out: ip.String() + ":8000",
		})
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.in, func(t *testing.T) {
			lis, err := NewStoppableListener(context.Background(), tt.in)
			if err != nil {
				t.Skipf("skip listener error: %v", err)
			}
			defer func() {
				require.NoError(t, lis.Close())
			}()

			addrs, err := GetListenerAddrs(lis.Addr())
			if err != nil {
				t.Error(err)
			}
			if len(tt.out) == 0 {
				t.Logf("-> %v", addrs)
				if len(addrs) < tt.minOutLen {
					t.Errorf("%s error: %v", tt.in, addrs)
				}
			} else {
				if tt.minOutLen <= len(addrs) {
					if len(addrs) == 0 {
						t.Logf("%s expected_minoutlen=%d actual_outlen=%d",
							tt.in, tt.minOutLen, len(addrs))
						return
					}

					for _, addr := range addrs {
						if addr == tt.out {
							return
						}
					}
				}

				t.Errorf("%s expected=%s actual=%s", tt.in, tt.out, addrs)
			}
		})
	}
}

func TestIPs(t *testing.T) {
	ips, err := IPs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ips) == 0 {
		t.Errorf("no ip?")
	}
	for _, ip := range ips {
		if ip.IsUnspecified() {
			t.Errorf("%v: unspecified ip", ip)
		}
		if ip.IsMulticast() {
			t.Errorf("%v: multicast ip", ip)
		}
	}
}

func TestAdvertisableIPs(t *testing.T) {
	uniIps, err := AdvertisableIPs()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(uniIps)
}

func TestNodeIDGen(t *testing.T) {
	const port = 10000

	ips, err := IPs()
	if err != nil {
		t.Fatal(err)
	}

	for _, ip := range ips {
		addr := ip.String() + ":" + strconv.Itoa(port)
		nodeID := types.NewNodeID(addr)
		t.Logf("addr=%v nodeid=%v", addr, nodeID)
	}

}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
