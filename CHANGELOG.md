# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
<<<<<<< HEAD
- Added `--server-read-buffer-size` option to storage node. (#VARLOG-671/#591)
- Added `--server-write-buffer-size` option to storage node. (#VARLOG-671/#591)
- Added `--replication-client-read-buffer-size` option to storage node. (#VARLOG-671/#591)
- Added `--replication-client-write-buffer-size` option to storage node. (#VARLOG-671/#591)
- Added `--reportcommitter-read-buffer-size` option to metadata repository. (#VARLOG-671/#591)
- Added `--reportcommitter-write-buffer-size` option to metadata repository. (#VARLOG-671/#591)
- Added pool of `internal/storagenode/replication.(*requestTask)` to lower heap allocations of replication request tasks. (#VARLOG-672/#592)

### Changed
- Fixed bug of `--volumes` arguments in `bin/start_varlogsn.py` again. (#VARLOG-670/#589)
- Reuse `proto/snpb/ReplicationRequest` whenever calling Replicate RPCs. (#VARLOG-672/#592)
- Change callbacks of Replicate RPC from dynamically generated closures to pre-defined methods to avoid excessive heap allocations. (#VARLOG-672/#592)


## [0.1.4] - 2022-01-11
### Added
- Added the OpenTelemetry to k8s manifests that is for the experiment. (#VARLOG-649/#563)
- Added `--batch-size` option to `varlogcli` to set the size of append batch. (#VARLOG-653/570)
- Added metrics about the delay between reports and commits in the metadata repository. (#VARLOG-654/#571)
- Added metrics about the various performance indices in the storage node. (#VARLOG-656/#573)
- Added `--ballast-size` option to storage node. (#VARLOG-669/#585)
- Added new replica selector `internal/varlogadm.(*balancedReplicaSelector)` that makes balance of replicas and primary replicas across storage nodes. (#VARLOG-657/#577)

### Changed
- Fixed wrong URL in `CHANGELOG.md`. (#VARLOG-652/#569)
- Renamed existing metrics in the storage node to represent their purposes. (#VARLOG-656/#573)
- Renamed package `internal/vms` to `internal/varlogadm` and `cmd/vms` to `cmd/varlogadm`. (#VARLOG-663/#576)
- Used pool of `internal/storagenode/replication.(*replicateTask)`. (#VARLOG-660/#575)
- Wait for only enqueueing writeTask into writeQueue in backup replica rather than waiting for completion of disk I/O. (#VARLOG-660/#575)
- Script `bin/start_varlogsn.py` accepts multiple of `--volumes` arguments correctly. (#VARLOG-670/#587)
- Call `internal/storagenode/replication.(Client).Replicate` sequentially in replicator. (#VARLOG-666/#582)
- Reuse `proto/snpb.ReplicationResponse` whenever receiving and sending the response in server and client. (#VARLOG-667/#583)

### Removed
- Removed the metric `sn.write.report.delay`. (#VARLOG-665/#581)


## [0.1.3] - 2021-12-30
### Added
- Release `internal/storagenode/executor.(*writeTask)` and `internal/storagenode/executor.(*taskWaitGroup)` to pool after appended. (#VARLOG-650/#565)

### Changed
- Bump `github.com/cockroachdb/pebble` from v0.0.0-20210817201821-5e4468e97817 to v0.0.0-20211222161641-06e42cfa82c0. (#VARLOG-651/#566)


## [0.1.2] - 2021-12-30
### Added
- Implemented seal and unseal subcommand in `varlogctl`. (#VARLOG-646/#564)

### Changed
- Use `go.opentelemetry.io/otel/sdk/metric/selector/simple.NewWithHistogramDistribution` as default aggregatorSelector. (#VARLOG-648/#562)
- Changed initialization of `go.opentelemetry.io/otel/sdk/resource.(Resource)` in `cmd/storagenode/app.initMeterProvider`. (#VARLOG-648/#562)


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


[Unreleased]: https://github.com/kakao/varlog/compare/v0.1.4...HEAD
[0.1.4]: https://github.com/kakao/varlog/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/kakao/varlog/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/kakao/varlog/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/kakao/varlog/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/kakao/varlog/releases/tag/v0.1.0
