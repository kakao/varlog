package storagenode

var (
	grpcHandlerLogAllowList map[string]bool
	grpcPayloadLogAllowList map[string]bool
)

func init() {
	grpcHandlerLogAllowList = make(map[string]bool)
	grpcPayloadLogAllowList = make(map[string]bool)

	for _, method := range []string{
		"/varlog.snpb.Management/AddLogStreamReplica",
		"/varlog.snpb.Management/RemoveLogStream",
		"/varlog.snpb.Management/Seal",
		"/varlog.snpb.Management/Unseal",
		"/varlog.snpb.Management/Sync",
		"/varlog.snpb.Management/Trim",
		"/varlog.snpb.LogIO/Read",
		"/varlog.snpb.LogIO/Subscribe",
		"/varlog.snpb.LogIO/SubscribeTo",
		"/varlog.snpb.LogIO/TrimDeprecated",
		"/varlog.snpb.Replicator/SyncInit",
	} {
		grpcHandlerLogAllowList[method] = true
	}

	for _, method := range []string{
		"/varlog.snpb.Management/RemoveLogStream",
		"/varlog.snpb.Management/Seal",
		"/varlog.snpb.Management/Sync",
		"/varlog.snpb.Management/Trim",
		"/varlog.snpb.LogIO/Read",
		"/varlog.snpb.LogIO/Subscribe",
		"/varlog.snpb.LogIO/SubscribeTo",
		"/varlog.snpb.LogIO/TrimDeprecated",
		"/varlog.snpb.Replicator/SyncInit",
	} {
		grpcPayloadLogAllowList[method] = true
	}
}
