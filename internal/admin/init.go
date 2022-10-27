package admin

var (
	grpcHandlerLogDenyList map[string]bool
)

func init() {
	grpcHandlerLogDenyList = make(map[string]bool)

	for _, method := range []string{
		"/grpc.health.v1.Health/Check",
	} {
		grpcHandlerLogDenyList[method] = true
	}
}
