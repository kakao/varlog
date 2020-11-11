package mrpb

//go:generate mockgen -build_flags -mod=vendor -package mock -destination mock/mrpb_mock.go . ManagementClient,ManagementServer,MetadataRepositoryServiceClient,MetadataRepositoryServiceServer
