package snpb

//go:generate go tool mockgen -build_flags -mod=vendor -package mock -destination mock/snpb_mock.go . ReplicatorClient,ReplicatorServer,Replicator_ReplicateClient,LogIOClient,LogIOServer,LogIO_AppendClient,LogIO_SubscribeClient,LogIO_SubscribeServer,LogStreamReporterClient,LogStreamReporterServer,ManagementClient,ManagementServer
