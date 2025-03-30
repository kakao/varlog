package admpb

//go:generate go tool mockgen -build_flags -mod=vendor -package admpb -destination admin_mock.go . ClusterManagerClient,ClusterManagerServer
