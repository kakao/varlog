# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Operation script `start_varlogsn.py` will not run the storage node if there are any missing data directories. (#VARLOG-765/#693)


## [0.1.14] - 2022-06-30
### Added
- Added initial tests for responses of admin protocols. (#VARLOG-747/#685)
- Added `--logfile-retention-days` flags to the varlogadm to set retention of log files in a unit of days. (#VARLOG-764/#690)
- Added `--logfile-compression` flags to the varlogadm to compress log files. (#VARLOG-764/#690)

## Changed
- Changed signatures of methods `pkg/varlog.(Admin).UnregisterTopic`, `pkg/varlog.(Admin).ListLogStreams` and `pkg/varlog.(Admin).ListMetadataRepositoryNodes`. (#VARLOG-761/#687)
- Changed output of varlogctl to show the results of RPCs directly. (#VARLOG-762/#688)
- The metadata repository of end-to-end tests running on k8s now uses StatefulSet rather than DaemonSet. (#VARLOG-763/#689)
- The file name of log files in the varlogadm is formatted by local time rather than UTC. (#VARLOG-764/#690)


## [0.1.13] - 2022-06-15
### Changed
- Responses of `GetStorageNode` and `ListStorageNodes` of the admin server are fixed to contain the log stream replicas. (#VARLOG-757/#682)


## [0.1.12] - 2022-06-15
### Added
- Added a new type `proto/vmspb.(StorageNodeMetadata)` to represent status and metadata for the storage node. (#VARLOG-758/#680)
- Added a new field `StartTime` to `proto/snpb.(StorageNodeMetadataDescriptor)` to represent start time of storage node process. (#VARLOG-758/#680)
- Added a new field `CreateTime` to `proto/varlogpb.(StorageNodeDescriptor)` to represent the time when the storage node is registered to the metadata repository. (#VARLOG-758/#680)

### Removed 
- Removed fields `CreatedTime` and `UpdatedTime` from `proto/snpb.(StorageNodeMetadataDescriptor)`. (#VARLOG-758/#680)


## [0.1.11] - 2022-06-13
### Added
- Added a new flag `--auto-unseal`, which enables auto unsealing for the sealed log stream, to the varlogadm CLI. (#VARLOG-757/#678)

### Changed
- Fixed to ignore zombie or not registered replicas in `internal/admin/stats.Repository`. (#VARLOG-756/#674)
- RPC `proto/snpb.(LogIOClient).LogStreamMetadata` returns metadata of log stream even if the log stream is sealed. (#VARLOG-754/#676)


## [0.1.10] - 2022-06-09
### Added
- Added `pkg/rpc.(*Manager)` to manage gRPC connections. (#VARLOG-739/#660)
- Field `proto/varlogpb.(StorageNodeDescriptor).Paths` is added to `proto.varlogpb.(StorageNodeDescriptor)` since it is immutable properties of the storage node. (#VARLOG-745/#664)
- New flag `--log-stream-gc-timeout`, which specifies the expiration duration of garbage log streams, is added to the varlogadm CLI. (#VARLOG-746/#667)
- New flag `--disable-auto-log-stream-sync`, which disables automatic sync between log stream replicas, is added to the varlogadm CLI. (#VARLOG-746/#667)
- New flag `--sn-watcher-heartbeat-check-deadline`, which sets the deadline for checking heartbeats of storage nodes, is added to the varlogadm CLI. (#VARLOG-746/#667)
- New flag `--sn-watcher-report-deadline`, which sets the deadline for reporting metadata of storage nodes, is added to the varlogadm CLI. (#VARLOG-746/#667)

### Changed
- Moved package `pkg/logclient` and `pkg/snc` to `internal/storagenode/client` to prevent exposing the package. (#VARLOG-743/#662)
- `internal/storagenode/client.(Manager)` manages both `internal/storagenode/client.(LogClient)` and `internal/storagenode/client.(ManagementClient)`. (#VARLOG-743/#662)
- The type of field `proto/varlogpb.(StorageNodeMetadataDescriptor).StorageNode` is changed from `varlogpb.StorageNodeDescriptor` to `varlogpb.StorageNode`. (#VARLOG-745/#664)
- The `proto/vmspb.(AddStorageNodeRequest)` now takes the ID as well as the address of the storage node. (#VARLOG-737/#665)
- The RPC `proto/vmspb.(ClusterManagerClient).AddStorageNode` is now idempotent - adding already added storage node is okay. (#VARLOG-737/#665)
- `internal/varlogadm.(StorageNodeManager)` is moved to `internal/varlogadm/snmanager.(StorageNodeManager)`. (#VARLOG-658/#666)
- `internal/varlogadm.(MetadataRepositoryManager)` is moved to `internal/varlogadm/mrmanager.(MetadataRepositoryManager)`. (#VARLOG-658/#666)
- `internal/varlogadm/snmanager.(StorageNodeManager)` and `internal/varlogadm/mrmanager.(MetadataRepositoryManager)` are injected to `internal/varlogadm.(ClusterManager)` rather than instantiating in the constructor of `internal/varlogadm.(ClusterManager)` to improve testability of varlog admin server. (#VARLOG-658/#666)
- Renamed `internal/varlogadm.(StatRepository)` to `internal/varlogadm/stats.(Repository)`. (#VARLOG-748/#668)
- Renamed `internal/varlogadm.(LogStreamStat)` to `internal/varlogadm/stats.(LogStreamStat)`. (#VARLOG-748/#668)
- Renamed `internal/varlogadm` to `internval/admin`. (#VARLOG-750/#669)
- Renamed `internal/varlogadm.(ClusterManager)` to `internval/admin.(Admin)`. (#VARLOG-750/#669)

### Removed
- Removed dead code - interface `internal/metarepos.(StorageNodeManagementClientFactory)` and its implementations and package `pkg/rpc/testpb`. (#VARLOG-743/#662)
- Field `proto/varlogpb.(StorageNodeDescriptor).Storages`, which is type of `proto/varlogpb.(StorageDescriptor)`, is removed since it is mutable. (#VARLOG-745/#664)


## [0.1.9] - 2022-05-11
### Added
- Added interface types for functional options in varlogadm, for instance, `internal/varlogadm.(Option)`, `internal/varlogadm.(MRManagerOption)` and `internal/varlogadm.(WatcherOption)`. (#VARLOG-740/#654)

### Removed
- Removed structs for options in varlogadm, for instance, `internal/varlogadm.(Options)`, `internal/varlogadm.(MRManagerOptions)` and `internal/varlogadm.(WatcherOptions)`. (#VARLOG-740/#654)


## [0.1.8] - 2022-05-04
### Added
- Added the fields `LocalLowWatermark` and `GlobalHighWatermark` to `proto/snpb.(LogStreamReplicaMetadataDescriptor).LocalHighWatermark`. (#VARLOG-719/#624)
- Added `proto/varlogpb.(TopicLogStream)`, `proto/varlogpb.(LogStreamReplica)` and `proto/varlogpb.(LogSequenceNumber)` to package `proto/varlogpb`. (#VARLOG-719/#624)
- Added `internal.storage.(*Storage).DiskUsage` to package `internal/storage`. (#VARLOG-722/#625)
- Added disk usages to the response of `proto/snpb.(ManagementServer).GetMetadata`. (#VARLOG-720/#628)

### Changed
- Merged `proto/varlogpb.(Replica)` and `proto/varlogpb.(LogStreamReplicaDescriptor)` into `proto/varlogpb.(LogStreamReplica)`. (#VARLOG-719/#624)
- Moved `proto/varlogpb.(StorageNodeMetadataDescriptor)` to `proto/snpb.(StorageNodeMetadataDescriptor)`. (#VARLOG-719/#624)
- Moved `proto/varlogpb.(LogStreamMetadataDescriptor)` to `proto/snpb.(LogStreamReplicaMetadataDescriptor)`. (#VARLOG-719/#624)
- Renamed `proto/snpb.(LogStreamReplicaMetadataDescriptor).HighWatermark` to `proto/snpb.(LogStreamReplicaMetadataDescriptor).LocalHighWatermark`. (#VARLOG-719/#624)
- Enriched the response of `proto/vmspb.(ClusterManager).GetStorageNodes` RPC. (#VARLOG-723/#630)
- Renamed package `pkg/logc` to `pkg/logclient`. (#VARLOG-701/#614)
- Renamed `pkg/logclient.(LogClientManager)` to `pkg/logclient.(Manager)`. (#VARLOG-702/#615)
- Changed `pkg/logclient.(Manager)` from interface to structure. (#VARLOG-702/#615)
- Removed unnecessary singleflight from `pkg/logclient.(Manager)`. (#VARLOG-703/#616)
- Changed package `cmd/metadata_repository` to `cmd/varlogmr`. (#VARLOG-731/#640)
- Changed package `internal/metadata_repository` to `internal/metarepos`. (#VARLOG-731/#640)
- Changed interface `pkg/logclient.(LogIOClient)` to struct `pkg/logclient.(Client)`. (#VARLOG-704/#647)

### Removed
- Removed unnecessary `pkg/logclient.(logClientProxy)`. (#VARLOG-704/#647)


## [0.1.7] - 2022-04-06
### Added
- Added `--server-max-msg-size` option to storage node. (#VARLOG-688/#601)
- Added `--logdir` and `--logtostderr` options to varlogadm. (#VARLOG-692/#604)
- Added Trim RPC to package `proto/vmspb`. (#VARLOG-706/#608)
- Added Trim RPC to `proto/snpb/management.proto`. (#VARLOG-708/#609)
- Added `Trim` method to `pkg/snc.(StorageNodeManagementClient)`. (#VARLOG-708/#609)
- Added `Trim` method to `internal/storagenode.(adminServer)`. (#VARLOG-708/#609,#VARLOG-711/#611)
- Implemented `Trim` in varlogadm. (#VARLOG-707/#612)
- Added logging settings to `bin/start_varlogadm.py`. (#VARLOG-713/#617)
- Added `Trim` method to `pkg/varlog.(Admin)`. (#VARLOG-712/#613)


### Changed
- Changed default storage settings for good performance in usual cases. (#VARLOG-668/#584)
- Redesigned and reimplemented `internal/storagenode` and `internal/storage`. (#VARLOG-681/#600)
- Fixed data race issue that can occur in the sequencer when the log stream replica is sealed. (#VARLOG-691/#602)
- Rearrange the order of sending tasks to the committer and sending tasks to the writer in the sequencer. (#VARLOG-693/#603)

### Removed
- Removed state machine log from metadata repository. (#VARLOG-687/#606)


## [0.1.6] - 2022-02-09
### Changed
- Fixed a bug that SubscribeTo API returns an invalid range error while waiting for new logs committed. (#VARLOG-683/#598)


## [0.1.5] - 2022-01-19
### Added
- Added `--server-read-buffer-size` option to storage node. (#VARLOG-671/#591)
- Added `--server-write-buffer-size` option to storage node. (#VARLOG-671/#591)
- Added `--replication-client-read-buffer-size` option to storage node. (#VARLOG-671/#591)
- Added `--replication-client-write-buffer-size` option to storage node. (#VARLOG-671/#591)
- Added `--reportcommitter-read-buffer-size` option to metadata repository. (#VARLOG-671/#591)
- Added `--reportcommitter-write-buffer-size` option to metadata repository. (#VARLOG-671/#591)
- Added pool of `internal/storagenode/replication.(*requestTask)` to lower heap allocations of replication request tasks. (#VARLOG-672/#592)
- Compare local high watermark with the end of the requested range in subscribeTo. (#VARLOG-676/#595)

### Changed
- Fixed bug of `--volumes` arguments in `bin/start_varlogsn.py` again. (#VARLOG-670/#589)
- Reuse `proto/snpb/ReplicationRequest` whenever calling Replicate RPCs. (#VARLOG-672/#592)
- Change callbacks of Replicate RPC from dynamically generated closures to pre-defined methods to avoid excessive heap allocations. (#VARLOG-672/#592)
- Reuse pre-defined buffer to encode data key and commit key while creating write batch and commit batch. (#VARLOG-673/#593)
- Writing logs and sending them to internal queues runs in separate goroutines. (#VARLOG-675/#594)


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


[Unreleased]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.14...HEAD
[0.1.14]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.13...v0.1.14
[0.1.13]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.12...v0.1.13
[0.1.12]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.11...v0.1.12
[0.1.11]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.10...v0.1.11
[0.1.10]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.9...v0.1.10
[0.1.9]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.daumkakao.com/varlog/varlog/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.daumkakao.com/varlog/varlog/releases/tag/v0.1.0
