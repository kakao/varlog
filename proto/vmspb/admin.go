package vmspb

//go:generate mockgen -build_flags -mod=vendor -package vmspb -destination admin_mock.go . ClusterManagerClient,ClusterManagerServer
