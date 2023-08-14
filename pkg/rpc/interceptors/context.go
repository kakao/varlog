package interceptors

import (
	"context"
	"net"
	"strings"

	"google.golang.org/grpc/peer"
)

func PeerAddress(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return ""
	}

	if host == "" {
		return "127.0.0.1"
	}
	return host
}

// ParseFullMethod returns service and method extracted from gRPC full method
// string, i.e., /package.service/method.
func ParseFullMethod(fullMethod string) (service, method string) {
	name, found := strings.CutPrefix(fullMethod, "/")
	if !found {
		// Invalid format, does not follow `/package.service/method`.
		return "", ""
	}
	service, method, found = strings.Cut(name, "/")
	if !found {
		// Invalid format, does not follow `/package.service/method`.
		return "", ""
	}
	if strings.Contains(method, "/") {
		// Invalid format, does not follow `/package.service/method`.
		return "", ""
	}
	return service, method
}
