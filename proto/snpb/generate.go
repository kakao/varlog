package snpb

//go:generate mockgen -build_flags -mod=vendor -package mock -destination mock/snpb_mock.go . ReplicatorClient,ReplicatorServer,Replicator_ReplicateClient,Replicator_ReplicateServer,LogIOClient,LogIOServer,LogIO_SubscribeClient,LogIO_SubscribeServer,LogStreamReporterClient,LogStreamReporterServer,ManagementClient,ManagementServer
