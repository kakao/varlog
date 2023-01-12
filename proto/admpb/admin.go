package admpb

//go:generate mockgen -build_flags -mod=vendor -package admpb -destination admin_mock.go . ClusterManagerClient,ClusterManagerServer
