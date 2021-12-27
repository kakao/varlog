# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]


## [0.1.1] - 2021-12-27
### Added
- Added `CHANGELOG.md`. (#VARLOG-631/#548)
- Added `mrtool` to retrieve metadata of MR without VMS. (#VARLOG-612/#546)
- Added a new k8s manifests - `deploy/k8s-experiment` that changes MR from DaemonSet to StatefulSet. (#VARLOG-612/#546)
- Added a new MR launcher - `bin/start_varlogmr.py` that uses `mrtool`. (#VARLOG-612/#546)
- Added per-logstream mutex to `internal/vms.(*clusterManager)` to serialize various RPC methods and the method to check statuses of log streams (`internal/vms.(*clusterManager).checkLogStreamStatus`). (#VARLOG-383/#549)
- Added a CGO_CFLAGS when building in mac os to Makefile. (#VARLOG-635/#551)
- Added `DescribeTopic(context.Context, types.TopicID) (*vmspb.DescribeTopicResponse, error)` method to `pkg/varlog.(Admin)` (#VARLOG-632/#550)
- Added `DescribeTopic` unary RPC to `proto/vmspb` (#VARLOG-632/#550)
- Added arguments to python scripts that start metadata repository, storage node, and admin servers. (#VARLOG-638/#554)
- Added `varlogcli` to produce and consume in the console. (#VARLOG-647/#558)

### Changed
- Use the non-blocking dial option to connect OpenTelemetry agent via OTLP. (#VARLOG-637/#552)
- Initialize log stream id generator without refreshing which calls RPC to storage nodes. (#VARLOG-634/#553)
- Bump `github.com/open-telemetry/opentelemetry-go` from v1.0.0/v0.23.0 to v1.3.0/v0.27.0 (#VARLOG-643/#556)

### Removed
- Remove unnecessary file `TOPIC`. (#VARLOG-631/#548)


## [0.1.0] - 2021-12-15


[Unreleased]: https://github.com/kakao/varlog/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/olivierlacan/keep-a-changelog/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/kakao/varlog/releases/tag/v0.1.0
