# Changelog

## [0.11.0](https://github.com/kakao/varlog/compare/v0.10.0...v0.11.0) (2023-02-20)


### Features

* **admin:** defines error codes of several RPCs in the admin server ([f5ed66f](https://github.com/kakao/varlog/commit/f5ed66ff776db710bd13e5782de404f6c888cae6)), closes [#312](https://github.com/kakao/varlog/issues/312)
* **metarepos:** add grpc error codes to the metadata repository service ([2903f8c](https://github.com/kakao/varlog/commit/2903f8cbc438653a92cb855d991ef78e6845142f))
* **metarepos:** fix grpc error code ([ce5feb3](https://github.com/kakao/varlog/commit/ce5feb36392b26f729da9d8fe0c29ed01bef5bdc))
* **storagenode:** add gRPC error codes to the admin service ([80cd082](https://github.com/kakao/varlog/commit/80cd08283fcc5eb36e60340c7a24ad56cb43556c)), closes [#312](https://github.com/kakao/varlog/issues/312)
* **storagenode:** add gRPC error codes to the log server ([5e813fc](https://github.com/kakao/varlog/commit/5e813fce4fc87fd0ce0c82ecbd8c16d409194cce)), closes [#312](https://github.com/kakao/varlog/issues/312)
* **varlogcli:** do not deny filtered logsteam ([26fec68](https://github.com/kakao/varlog/commit/26fec688596bf059b5cc0f42d04cdf05fff5a74c))
* **varlogcli:** select log stream within AllowedLogStreams ([26627a0](https://github.com/kakao/varlog/commit/26627a09c53891855c56cb55b5b20f643f555eb8))

## [0.10.0](https://github.com/kakao/varlog/compare/v0.9.2...v0.10.0) (2023-01-04)


### Features

* **admin:** return ResourceExhausted if the log streams count overflows ([b15f29e](https://github.com/kakao/varlog/commit/b15f29e7253c2986050facbe2c3b289d195deb3c))
* **admin:** Updates are rejected if there is no sealed replica ([25cef3d](https://github.com/kakao/varlog/commit/25cef3d037fb42cf5ab5c456fd4a57716cc438ea))
* **admin:** Updates are rejected if there is no sealed replica ([eef3b9f](https://github.com/kakao/varlog/commit/eef3b9f2451720511b7aa8ac278142a7c6f9545f))
* **metarepos:** add an upper limit for the number of log streams in a topic ([ad2a60f](https://github.com/kakao/varlog/commit/ad2a60fb71efff96835e214a4b738a880442642b)), closes [#297](https://github.com/kakao/varlog/issues/297)
* **metarepos:** add an upper limit for the number of topics in a cluster ([77c6ee4](https://github.com/kakao/varlog/commit/77c6ee4b3e2419d7c273f1130a67bb340a8a689e)), closes [#295](https://github.com/kakao/varlog/issues/295)
* **metarepos:** Prevent log loss due to UpdateLogStream ([c319333](https://github.com/kakao/varlog/commit/c319333ad93c23c9f25ea33abf347632bf1d8504))
* **storagenode:** add a new sync state - SyncStateStart ([b44fd55](https://github.com/kakao/varlog/commit/b44fd5508904c2c1431834bb6b6e0a06aaad5c62)), closes [#299](https://github.com/kakao/varlog/issues/299)
* **storagenode:** add an upper limit of log stream replicas count in a storage node ([2cfc8bf](https://github.com/kakao/varlog/commit/2cfc8bf8364092d3d93c21f49f544431ddde3a7e)), closes [#293](https://github.com/kakao/varlog/issues/293)


### Bug Fixes

* **benchmark:** consider multi-target workloads in benchmark webapp ([56f338e](https://github.com/kakao/varlog/commit/56f338e261c38dde049d250107d9e9fe75e79ff6))

## [0.9.2](https://github.com/kakao/varlog/compare/v0.9.1...v0.9.2) (2022-12-16)


### Bug Fixes

* **benchmark:** fix sql for target and workload ([cdc1268](https://github.com/kakao/varlog/commit/cdc1268e03a99e6dc104a25c8208a26e351dd713))

## [0.9.1](https://github.com/kakao/varlog/compare/v0.9.0...v0.9.1) (2022-12-16)


### Bug Fixes

* **benchmark:** create a new row and get the row from the tables ([760aef4](https://github.com/kakao/varlog/commit/760aef434ce2db3623e8f4b15bb7988f276e996d))

## [0.9.0](https://github.com/kakao/varlog/compare/v0.8.1...v0.9.0) (2022-12-14)


### Features

* **benchmark:** add `--print-json` to print benchmark result as JSON ([abf8a5a](https://github.com/kakao/varlog/commit/abf8a5af53b7ca5f916dc2b23f453279ea9ed443)), closes [#257](https://github.com/kakao/varlog/issues/257)
* **benchmark:** add `save` command to benchmark ([25ecb80](https://github.com/kakao/varlog/commit/25ecb80a7424ac1805fac76eded5dbd44149a57b)), closes [#257](https://github.com/kakao/varlog/issues/257)
* **benchmark:** add a `test` command to the benchmark tool ([0e08249](https://github.com/kakao/varlog/commit/0e082492cba29fc83895570e10f2e41d9437be2a)), closes [#257](https://github.com/kakao/varlog/issues/257)
* **benchmark:** add initdb command to initialize benchmark database ([9d115eb](https://github.com/kakao/varlog/commit/9d115eb93a4182d5b30d4a6315de51b49df1efe3)), closes [#264](https://github.com/kakao/varlog/issues/264)
* **client,storagenode:** remove Head and Tail from proto/varlogpb.(LogStreamDescriptor) ([57161c8](https://github.com/kakao/varlog/commit/57161c832168957b46960186d33d172304f4330f)), closes [#73](https://github.com/kakao/varlog/issues/73)


### Bug Fixes

* **storagenode:** fix concurrency bugs of settings for storage and executor ([fdd1781](https://github.com/kakao/varlog/commit/fdd17814ff91c749e634acca6ff0d4118245594d)), closes [#262](https://github.com/kakao/varlog/issues/262)

## [0.8.1](https://github.com/kakao/varlog/compare/v0.8.0...v0.8.1) (2022-12-07)


### Bug Fixes

* **storagenode,client:** add missing mock files ([436f0d9](https://github.com/kakao/varlog/commit/436f0d9ba5fbfe00beda982ea54b646876654351))

## [0.8.0](https://github.com/kakao/varlog/compare/v0.7.1...v0.8.0) (2022-12-06)


### Features

* **client:** add PeekLogStream to the client ([e872677](https://github.com/kakao/varlog/commit/e8726770ce2c7e0ae465f88a16a9bc4a98cb31d0)), closes [#239](https://github.com/kakao/varlog/issues/239)
* **storagenode:** add SyncReplicateStream to synchronize replicas by using stream gRPC ([d8d7888](https://github.com/kakao/varlog/commit/d8d788877976bdb0bb5e022068084fb674555f4a)), closes [#241](https://github.com/kakao/varlog/issues/241)

## [0.7.1](https://github.com/kakao/varlog/compare/v0.7.0...v0.7.1) (2022-11-23)


### Bug Fixes

* **dockerfile:** use go 1.19 ([923f35f](https://github.com/kakao/varlog/commit/923f35f0e00d90ea436ec3ae57df4fb9edf1cd3e))

## [0.7.0](https://github.com/kakao/varlog/compare/v0.6.0...v0.7.0) (2022-11-22)


### Features

* **benchmark:** rework benchmark ([9c3f84a](https://github.com/kakao/varlog/commit/9c3f84a4780f2295de4b3d430a278d62fc62ed8f)), closes [#209](https://github.com/kakao/varlog/issues/209)
* define CommitBatchResponse ([6046c99](https://github.com/kakao/varlog/commit/6046c9941f6cfaa3d0aec870b0c19e5a8c41b405))
* **storage:** introduce append batch ([0534ad0](https://github.com/kakao/varlog/commit/0534ad0a754fa0e728b1e9571ac121db8d8ad557)), closes [#125](https://github.com/kakao/varlog/issues/125)
* **storagenode:** change the synchronization method to accept only the last commit context ([8d331f6](https://github.com/kakao/varlog/commit/8d331f6a938d0422456ce746ba8eaba05457ac2c)), closes [#125](https://github.com/kakao/varlog/issues/125)
* **storagenode:** make log stream executor sealing when reportCommitBase is invalid ([6f90720](https://github.com/kakao/varlog/commit/6f907209382904ea3a50bb819c0a5e1eb6aee2dd)), closes [#125](https://github.com/kakao/varlog/issues/125)
* **storagenode:** remove bad data dirs and deprecate `--data-dirs` and `--volume-strict-check` ([1972c94](https://github.com/kakao/varlog/commit/1972c94b9831d07ff3c83d1f62f2c6621efc267b)), closes [#215](https://github.com/kakao/varlog/issues/215)
* unify uncommittedLLSNBegin and localHighWatermark in logStreamContext ([de88bcf](https://github.com/kakao/varlog/commit/de88bcfa0442034b7295b7f5b5e4d11b22128242))


### Performance Improvements

* **storage:** use a lightweight method to get the data size of the storage ([0965fd9](https://github.com/kakao/varlog/commit/0965fd9e1d300449ce44f4bb95601d7ef4efb549)), closes [#210](https://github.com/kakao/varlog/issues/210)

## [0.6.0](https://github.com/kakao/varlog/compare/v0.5.0...v0.6.0) (2022-10-12)


### Features

* **client:** deny lsid if not allowed ([c8ad568](https://github.com/kakao/varlog/commit/c8ad568979db3cd5db525071dc6bffa0f99396ec))

## [0.5.0](https://github.com/kakao/varlog/compare/v0.4.1...v0.5.0) (2022-10-07)


### Features

* **client:** add allowed logstream list option to varlog client ([1ce80e7](https://github.com/kakao/varlog/commit/1ce80e773dfc1cf77acc0e312afb303dd6abb1b4))
* **metarepos:** ignore invalid report ([de83a08](https://github.com/kakao/varlog/commit/de83a0869b5f43bf84b27e0b6f0df7fc81a77c76))
* **storage:** change the trim not to remove the commit context ([b80964b](https://github.com/kakao/varlog/commit/b80964b5c27c95f99a2a747b2620b91ba2fdb8b5)), closes [#125](https://github.com/kakao/varlog/issues/125)
* **storagenode:** replica in the learning state does not report to a metadata repository ([ca4c184](https://github.com/kakao/varlog/commit/ca4c184bd1a84088e63f4369e29126cd10ddbd15)), closes [#125](https://github.com/kakao/varlog/issues/125)
* **storagenode:** restore the status of a log stream replica by using the latest commit context ([9e042d2](https://github.com/kakao/varlog/commit/9e042d2f1ded63400f7f1e13322c2bfe65bfcd5b)), closes [#125](https://github.com/kakao/varlog/issues/125)
* **storagenode:** store only the latest commit context for every commit ([ecf3a12](https://github.com/kakao/varlog/commit/ecf3a120ed6a5a7112676afc0ec322a69d5caacb)), closes [#125](https://github.com/kakao/varlog/issues/125)


### Bug Fixes

* **storagenode:** fix error-prone state management of SyncInit and Report ([c7366c5](https://github.com/kakao/varlog/commit/c7366c5fe98175922a91b83eab3c1ac8ef155fdf))
* **storagenode:** remove data directory in removing log stream replica ([27fb13f](https://github.com/kakao/varlog/commit/27fb13f6c3310a79ad2b8e2990ab738fe8c15224)), closes [#157](https://github.com/kakao/varlog/issues/157)

## [0.4.1](https://github.com/kakao/varlog/compare/v0.4.0...v0.4.1) (2022-09-13)


### Bug Fixes

* **storagenode:** do not log payload of `/varlog.snpb.Management/AddLogStreamReplca` ([2cdb74a](https://github.com/kakao/varlog/commit/2cdb74a83f57a735627c353c8b8764c42f7ed138))

## [0.4.0](https://github.com/kakao/varlog/compare/v0.3.1...v0.4.0) (2022-09-12)


### Features

* add `proto/varlogpb.(ReplicaDescriptor).DataPath` ([4db1605](https://github.com/kakao/varlog/commit/4db16050c0744755f5bdf054d84979e598c23f56)), closes [#124](https://github.com/kakao/varlog/issues/124)


### Bug Fixes

* **admin:** succeed to `/varlog.vmspb.ClusterManager/UpdateLogStream` when already updated ([3c451a1](https://github.com/kakao/varlog/commit/3c451a1373028c85a1eb731cea80500693a3912e)), closes [#126](https://github.com/kakao/varlog/issues/126)
* **storagenode:** fix a response of SyncInit when source replica was trimmed ([72e152a](https://github.com/kakao/varlog/commit/72e152a01529281123695295d260c906b9c3c0fc)), closes [#134](https://github.com/kakao/varlog/issues/134)


### Performance Improvements

* **storagenode:** load log streams concurrently ([a59068e](https://github.com/kakao/varlog/commit/a59068e8f2da3476e6030c65013b315319c9aaa9)), closes [#138](https://github.com/kakao/varlog/issues/138)

## [0.3.1](https://github.com/kakao/varlog/compare/v0.3.0...v0.3.1) (2022-09-06)


### Bug Fixes

* **admin:** add log stream replicas to result of ListStorageNodes ([9bce94c](https://github.com/kakao/varlog/commit/9bce94c76773e57c8d2c977add726ef1d7bb7913)), closes [#106](https://github.com/kakao/varlog/issues/106)
* **storagenode:** make volumes absolute ([edfd550](https://github.com/kakao/varlog/commit/edfd550a74b45416c400f97b1ee084e65e1c4b45)), closes [#116](https://github.com/kakao/varlog/issues/116)

## [0.3.0](https://github.com/kakao/varlog/compare/v0.2.3...v0.3.0) (2022-09-04)


### Features

* **admin:** use gRPC codes to clarify errors returned from `proto/vmspb.UpdateLogStream` RPC ([8f539e1](https://github.com/kakao/varlog/commit/8f539e148d6736f881fae23cf5ae60ba47550c41)), closes [#107](https://github.com/kakao/varlog/issues/107)


### Bug Fixes

* **admin:** contains all storage nodes registered to the cluster ([28c02ab](https://github.com/kakao/varlog/commit/28c02ab7d4ac0bfa59bffa121a54b3a3aa29deb4)), closes [#106](https://github.com/kakao/varlog/issues/106)

## [0.2.3](https://github.com/kakao/varlog/compare/v0.2.2...v0.2.3) (2022-08-29)


### Bug Fixes

* **storage:** apply WithoutSync option correctly ([2c25007](https://github.com/kakao/varlog/commit/2c25007d0deaa24383e90d922ebc65ab4470f117)), closes [#102](https://github.com/kakao/varlog/issues/102)

## [0.2.2](https://github.com/kakao/varlog/compare/v0.2.1...v0.2.2) (2022-08-29)


### Bug Fixes

* add error handling to WalkFunc in filepath.Walk in `pkg/util/fputil.DirectorySize` ([ad4e5d8](https://github.com/kakao/varlog/commit/ad4e5d880ed7ad97c20bf21cba4e7fb88b7e3753))
* set empty list to `logStreams` when it is null ([0ee5627](https://github.com/kakao/varlog/commit/0ee56274e5b7fb4b96d78ebbe041bf967bc83e63)), closes [#88](https://github.com/kakao/varlog/issues/88)

## [0.2.1](https://github.com/kakao/varlog/compare/v0.2.0...v0.2.1) (2022-08-29)


### Bug Fixes

* **admin:** set empty slice to `logStreams` in GetStorageNode response rather than null when no log stream replicas ([200511e](https://github.com/kakao/varlog/commit/200511e6037198886c0827bd08b731023a4987d7)), closes [#88](https://github.com/kakao/varlog/issues/88)
* **mr:** handle duplicated RegisterLogStream([#62](https://github.com/kakao/varlog/issues/62)) ([861b3b0](https://github.com/kakao/varlog/commit/861b3b05c0670a2dcae6d6505eacdb21dc375134))

## [0.2.0](https://github.com/kakao/varlog/compare/v0.1.0...v0.2.0) (2022-08-25)


### Features

* **sn:** add CLI flags to varlogsn ([85c7f11](https://github.com/kakao/varlog/commit/85c7f11d42b703e11e6854a7937d29624c0c55cd))

## [0.1.0](https://github.com/kakao/varlog/compare/v0.0.3...v0.1.0) (2022-08-25)


### Features

* **admin:** add `--loglevel` flag to varlogadm ([77bfdf3](https://github.com/kakao/varlog/commit/77bfdf3b0207a6d4e3ff0471b5d40d39c80765a3)), closes [#79](https://github.com/kakao/varlog/issues/79)

## [0.0.3](https://github.com/kakao/varlog/compare/v0.0.2...v0.0.3) (2022-08-25)


### Bug Fixes

* **mr:** let newbie logstream know cur version ([cd12789](https://github.com/kakao/varlog/commit/cd12789f91fe1e4d17b14a8636535612a3fc793b))

## [0.0.2](https://github.com/kakao/varlog/compare/v0.0.1...v0.0.2) (2022-08-17)


### Bug Fixes

* **admin:** add handler timeout for failed sn ([a2f31d7](https://github.com/kakao/varlog/commit/a2f31d7b7b43a8522dd513ac824d040a6f515217)), closes [#29](https://github.com/kakao/varlog/issues/29)
* remove mutex in storage node manager of admin ([77ed718](https://github.com/kakao/varlog/commit/77ed7188883b488c48c89812548e9c6f5c889649)), closes [#30](https://github.com/kakao/varlog/issues/30)

## 0.0.1 (2022-08-14)


### Bug Fixes

* TestAdmin_GetStorageNode_FailedStorageNode ([#13](https://github.com/kakao/varlog/issues/13)) ([5c8a3c2](https://github.com/kakao/varlog/commit/5c8a3c234032e3bf647d2a5d10c9916c215a6d9b))


### Miscellaneous Chores

* release 0.0.1 ([#21](https://github.com/kakao/varlog/issues/21)) ([6aad0d8](https://github.com/kakao/varlog/commit/6aad0d80d7f3c00092d44bbcdad7730e6e956870))
