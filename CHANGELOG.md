# Changelog

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
