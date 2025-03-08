# Changelog

## [0.23.0](https://github.com/kakao/varlog/compare/v0.22.1...v0.23.0) (2025-03-08)


### Features

* **telemetry:** Support both new and deprecated OpenTelemetry runtime metrics ([b4acd3d](https://github.com/kakao/varlog/commit/b4acd3d4dcc86f39aeb031ec558c6de75327e154))

## [0.22.1](https://github.com/kakao/varlog/compare/v0.22.0...v0.22.1) (2024-07-04)


### Bug Fixes

* **sn:** prevent panic on closed queue channel of replication server ([6c19613](https://github.com/kakao/varlog/commit/6c196130a9b9faac53f7e5216d3b824060f704f7))

## [0.22.0](https://github.com/kakao/varlog/compare/v0.21.0...v0.22.0) (2024-06-15)


### Features

* **sn:** add metrics for Append and Replicate RPCs ([d52ff1d](https://github.com/kakao/varlog/commit/d52ff1d1f2bf24d47ef9f969520b51570c9abe19))

## [0.21.0](https://github.com/kakao/varlog/compare/v0.20.0...v0.21.0) (2024-06-11)


### Features

* **all:** add CLI flags for gRPC buffer management ([57db00e](https://github.com/kakao/varlog/commit/57db00e513b3ff49beb7544660b4fc8d308c8556))
* **storage:** add CLI flag '--storage-cache-size' for setting cache for storage ([e82fd37](https://github.com/kakao/varlog/commit/e82fd37d0a05b0632fee95a0a61618f22a5f828d))


### Bug Fixes

* **rpc:** use google.golang.org/grpc.NewClient instead of DialContext ([851f1c9](https://github.com/kakao/varlog/commit/851f1c9bcdd6e3d0e68612784090e67cb1dac918))

## [0.20.0](https://github.com/kakao/varlog/compare/v0.19.2...v0.20.0) (2024-03-05)


### Features

* **benchmark:** use gops to diagnose benchmark tool ([9f31816](https://github.com/kakao/varlog/commit/9f318169e229d62f5917b1d34f0b23123dc2a0b0))
* **benchmark:** use gops to diagnose benchmark tool ([#717](https://github.com/kakao/varlog/issues/717)) ([86a8155](https://github.com/kakao/varlog/commit/86a81553e16446bfb13fc9ecde54bab958249cdb))


### Bug Fixes

* **benchmark:** refine the benchmark tool for log subscription ([01bf188](https://github.com/kakao/varlog/commit/01bf188a22918ea3d57f72d61fff36525061d835))
* **benchmark:** refine the benchmark tool for log subscription ([#716](https://github.com/kakao/varlog/issues/716)) ([eb83d42](https://github.com/kakao/varlog/commit/eb83d428b8b47053fef8e9bff3d3031b416e4e22))


### Performance Improvements

* **benchmark:** use grpc.SharedBufferPool for grpc.DialOption ([8014ece](https://github.com/kakao/varlog/commit/8014ecec1bb1eb2f44da1d89af7d646dfa91b3c9))
* **benchmark:** use grpc.SharedBufferPool for grpc.DialOption ([#718](https://github.com/kakao/varlog/issues/718)) ([fbf0c17](https://github.com/kakao/varlog/commit/fbf0c17c84bef0556c6f3807d721ef55b187517d))
* **client:** optimize with atomic.Int64 for lastSubscribeAt in pkg/varlog.(subscriber) ([617bd9d](https://github.com/kakao/varlog/commit/617bd9d43429d5994a3d296dd1073d050e6bdda2))
* **client:** optimize with atomic.Int64 for lastSubscribeAt in pkg/varlog.(subscriber) ([#720](https://github.com/kakao/varlog/issues/720)) ([6d2779e](https://github.com/kakao/varlog/commit/6d2779eeb685215182aacfb5677d264a6dc41484))
* **client:** prealloc pkg/varlog.(*transmitter).transmitQueue ([58a5c21](https://github.com/kakao/varlog/commit/58a5c215bbf24cd3ef749660ddb9edb1d2ad1cdf))
* **client:** prealloc pkg/varlog.(*transmitter).transmitQueue ([#721](https://github.com/kakao/varlog/issues/721)) ([26ed1a6](https://github.com/kakao/varlog/commit/26ed1a6efc92809ea860036c061611d796689f66))
* **client:** reuse snpb.SubscribeResponse in RPC handler ([890508d](https://github.com/kakao/varlog/commit/890508d7e19ba32a1731ca817d467dc18a200128))
* **client:** reuse snpb.SubscribeResponse in RPC handler ([#719](https://github.com/kakao/varlog/issues/719)) ([e98ac54](https://github.com/kakao/varlog/commit/e98ac54496de0b7876d3a5b33ef328c3d44778b2))
* **storage:** enhance GLSN Log scanning efficiency ([963d10f](https://github.com/kakao/varlog/commit/963d10f2154f1d6e2c8649ee363cbf1024dbaf81))
* **storage:** improve scanning with GLSN ([#715](https://github.com/kakao/varlog/issues/715)) ([3caaa0a](https://github.com/kakao/varlog/commit/3caaa0a96df24fe845c66f8c7146c26c6e8b46e5))

## [0.19.2](https://github.com/kakao/varlog/compare/v0.19.1...v0.19.2) (2024-02-14)


### Bug Fixes

* **client:** remove subscribe's initial delay ([e77c763](https://github.com/kakao/varlog/commit/e77c76381801a1b85f61f5a9dd437e90cf14dfa4))

## [0.19.1](https://github.com/kakao/varlog/compare/v0.19.0...v0.19.1) (2024-02-01)


### Bug Fixes

* **varlogtest:** panic on PeekLogStream after removing all logs ([203fc33](https://github.com/kakao/varlog/commit/203fc33a18f1974bf0700990f6418a1733a63d74))
* **varlogtest:** panic on PeekLogStream after removing all logs ([#686](https://github.com/kakao/varlog/issues/686)) ([2b2cdfb](https://github.com/kakao/varlog/commit/2b2cdfb384a7b19a8a06d551c78b440c6370a523))

## [0.19.0](https://github.com/kakao/varlog/compare/v0.18.1...v0.19.0) (2024-01-02)


### Features

* **metarepos:** unseal reportCollector with LastCommittedVer - 1 ([d29c29a](https://github.com/kakao/varlog/commit/d29c29a044fb72ff7c1267620d0fa58bf9ab5741))
* pkg/util/units.FromByteSizeString respects SI and IEC standards ([cf0e749](https://github.com/kakao/varlog/commit/cf0e749bbf01079ae03a5f87b4842c7d49f114a9)), closes [#661](https://github.com/kakao/varlog/issues/661)
* pkg/util/units.FromByteSizeString respects SI and IEC standards ([#662](https://github.com/kakao/varlog/issues/662)) ([7e6a2f5](https://github.com/kakao/varlog/commit/7e6a2f586a519993ca7e199f2367cf5c2988c215)), closes [#661](https://github.com/kakao/varlog/issues/661)
* **storage:** handle inconsistency between data and commit in ReadRecoveryPoints ([8832c06](https://github.com/kakao/varlog/commit/8832c06a0a8c42aebaf482e80f59524abff4b98a))
* **storage:** handle inconsistency between data and commit in ReadRecoveryPoints ([#545](https://github.com/kakao/varlog/issues/545)) ([208c113](https://github.com/kakao/varlog/commit/208c11377bcf30a6f808d718ce2b591bc14b9fbf))

## [0.18.1](https://github.com/kakao/varlog/compare/v0.18.0...v0.18.1) (2023-10-17)


### Bug Fixes

* **varlogsn:** fix JSON parsing of get_data_dirs in start_varlogsn ([543d551](https://github.com/kakao/varlog/commit/543d55127e8d15ab7ef667a4faa8b3cee7af2bbf))

## [0.18.0](https://github.com/kakao/varlog/compare/v0.17.0...v0.18.0) (2023-10-10)


### Features

* **metarepos:** rename the executable from vmr to varlogmr ([0ef2d85](https://github.com/kakao/varlog/commit/0ef2d859da0ef64de68be546eb0b1aa5eed78f57))
* **metarepos:** rename the executable from vmr to varlogmr ([#610](https://github.com/kakao/varlog/issues/610)) ([52de516](https://github.com/kakao/varlog/commit/52de516f7547118ce697ad7ca525c833042cabca))


### Bug Fixes

* **e2e:** run the correct number of MR nodes ([b1e8432](https://github.com/kakao/varlog/commit/b1e84321f7cdd9d00370dc007cc0c4dd08e2e9c9))
* **e2e:** run the correct number of MR nodes ([#609](https://github.com/kakao/varlog/issues/609)) ([89a94e3](https://github.com/kakao/varlog/commit/89a94e337ea99ec99045ac84e337283102a3fc3a))
* **metarepos:** use --replication-factor to set the replication factor ([ef29008](https://github.com/kakao/varlog/commit/ef29008921ddf5301aa6b3de0b8d525555c7498d))
* **metarepos:** use --replication-factor to set the replication factor ([#608](https://github.com/kakao/varlog/issues/608)) ([1bdc345](https://github.com/kakao/varlog/commit/1bdc345c8ac16c690524ed555d3faa64f69133f6))

## [0.17.0](https://github.com/kakao/varlog/compare/v0.16.0...v0.17.0) (2023-10-10)


### Features

* **all:** add version to the daemons ([ab345de](https://github.com/kakao/varlog/commit/ab345de10a78c9d13b29e84ef356622aa1cf48fd))
* **all:** add version to the daemons ([#598](https://github.com/kakao/varlog/issues/598)) ([a430609](https://github.com/kakao/varlog/commit/a430609d62919f1565cf41744c8a6f2735ebe768))
* **varlogtest:** support ListLogStreams API ([c0c2e9d](https://github.com/kakao/varlog/commit/c0c2e9dafa8369ef24f0aa6682a7ffaa9bd0cbf3))
* **varlogtest:** support ListLogStreams API ([#607](https://github.com/kakao/varlog/issues/607)) ([1f42351](https://github.com/kakao/varlog/commit/1f42351570060415e2f41b37150a4799ebcd5372))

## [0.16.0](https://github.com/kakao/varlog/compare/v0.15.0...v0.16.0) (2023-10-04)


### Features

* **admin:** check cluster id in mrmanager ([b958fe0](https://github.com/kakao/varlog/commit/b958fe0f5615370b8e84320e5df1d4bb59181a98))
* **admin:** check cluster id in mrmanager ([#555](https://github.com/kakao/varlog/issues/555)) ([7794a6d](https://github.com/kakao/varlog/commit/7794a6dd9b6a50094fb1a3fdff0ff3a8c27e07ab))
* **admin:** check replication factor in mrmanager ([a18b285](https://github.com/kakao/varlog/commit/a18b28513d5f8e813ca9e9a88b038edea5526d3d))
* **admin:** check replication factor in mrmanager ([#556](https://github.com/kakao/varlog/issues/556)) ([2a1b5b6](https://github.com/kakao/varlog/commit/2a1b5b61ea3055c54e84ebf445ad4c238873a3b8))
* **all:** add gogoproto codec to grpc servers ([5e1f344](https://github.com/kakao/varlog/commit/5e1f344a849e9ab2b7871e082e7564dcb6365ab3))
* **all:** add gogoproto codec to grpc servers ([#581](https://github.com/kakao/varlog/issues/581)) ([6127cd2](https://github.com/kakao/varlog/commit/6127cd204e04f65ec0567f3150f5566cc92b8340))
* **all:** change type of ClusterID from uint32 to int32 ([500a4bb](https://github.com/kakao/varlog/commit/500a4bbfda2cfe99661c013872d729ca7772cca4))
* **all:** change type of ClusterID from uint32 to int32 ([#566](https://github.com/kakao/varlog/issues/566)) ([ac89e40](https://github.com/kakao/varlog/commit/ac89e40be8ac44026d8a1a26918736394215a7bf))
* **varlogtest:** add functional options to varlogtest ([97dab98](https://github.com/kakao/varlog/commit/97dab98428956e3552d85484cc3359c747f91566))
* **varlogtest:** add functional options to varlogtest ([#582](https://github.com/kakao/varlog/issues/582)) ([eb8a96f](https://github.com/kakao/varlog/commit/eb8a96f89665ff403fd4afa80d058a78bf5ef083))
* **varlogtest:** add initial metadata repository and support fetch APIs ([c033d62](https://github.com/kakao/varlog/commit/c033d621660acb0344d0378a913d40ccdac7970f))
* **varlogtest:** add initial metadata repository and support fetch APIs ([#583](https://github.com/kakao/varlog/issues/583)) ([626bdf8](https://github.com/kakao/varlog/commit/626bdf8a34762840be122ee8cdbb634ceb28eb45))
* **varlogtest:** create new instance of admin and log ([2c98f7f](https://github.com/kakao/varlog/commit/2c98f7fd8fc58458d590c360af6a80754988903e))
* **varlogtest:** create new instance of admin and log ([#591](https://github.com/kakao/varlog/issues/591)) ([385446d](https://github.com/kakao/varlog/commit/385446db861c9e53b0b9543c7914538064b6a9c5))


### Bug Fixes

* **admin:** refresh cluster metadata while adding storage node ([a2e0326](https://github.com/kakao/varlog/commit/a2e0326f13e7b844a0db3c8cc01469926e636844))
* **admin:** refresh cluster metadata while adding storage node ([#558](https://github.com/kakao/varlog/issues/558)) ([4cb06f1](https://github.com/kakao/varlog/commit/4cb06f1b391a5164e049ff074597f64f6b32d0f0))
* **rpc:** call Reset before unmarshaling protobuf ([aae2495](https://github.com/kakao/varlog/commit/aae24957d0e10a7af1c8c29bfba1e8b85d2d7ec0))
* **varlogtest:** log stream appender can be closed in the callback ([e08371b](https://github.com/kakao/varlog/commit/e08371b3338ec3f3b570d0da4d8bee2efb3d9d7c))
* **varlogtest:** log stream appender can be closed in the callback ([#595](https://github.com/kakao/varlog/issues/595)) ([2480922](https://github.com/kakao/varlog/commit/2480922be2637f227e65f2fb92e5ef6ae0ffc290))
* **varlogtest:** log stream appender returns missing errors in the callback ([9d0b203](https://github.com/kakao/varlog/commit/9d0b2034b04b4bdc455c4341e679f0b5899364d9))
* **varlogtest:** log stream appender returns missing errors in the callback ([#594](https://github.com/kakao/varlog/issues/594)) ([d2e88ee](https://github.com/kakao/varlog/commit/d2e88ee32ce8e90b288fed591d2c941cc2cd42df))
* **varlogtest:** validate log stream descriptor to create it ([73d6a91](https://github.com/kakao/varlog/commit/73d6a9168c9dc0194b5ec11390949270f532d779))
* **varlogtest:** validate log stream descriptor to create it ([#593](https://github.com/kakao/varlog/issues/593)) ([27cd3bb](https://github.com/kakao/varlog/commit/27cd3bb5a1e54fcdb16629fdbc794322752cb905))


### Performance Improvements

* **rpc:** use gogoproto codec ([#236](https://github.com/kakao/varlog/issues/236)) ([c4224c0](https://github.com/kakao/varlog/commit/c4224c05c1ceb2fabba8900475d2e24e51afba90))

## [0.15.0](https://github.com/kakao/varlog/compare/v0.14.1...v0.15.0) (2023-07-31)


### Features

* **admin:** add otelgrpc metric interceptor ([d9ca9aa](https://github.com/kakao/varlog/commit/d9ca9aaab6cdb56a096541ac3d8193a22558fa77))
* **admin:** add otelgrpc metric interceptor ([#509](https://github.com/kakao/varlog/issues/509)) ([db7a1a2](https://github.com/kakao/varlog/commit/db7a1a2467fc91346e6bb1324ed65d33ba7db3b2))
* **admin:** speed up fetching cluster metadata ([3e46f62](https://github.com/kakao/varlog/commit/3e46f625a8666d4c2aa077bcb90e5f57d701faaa))
* **admin:** speed up fetching cluster metadata ([#480](https://github.com/kakao/varlog/issues/480)) ([53a8f19](https://github.com/kakao/varlog/commit/53a8f195f6063add1c8ca6479f36acf5b6ab9102))
* **all:** add common flags for telemetry ([fcacd1a](https://github.com/kakao/varlog/commit/fcacd1a6491b1333fc0470f563ff651e9938707c))
* **all:** add common flags for telemetry ([#494](https://github.com/kakao/varlog/issues/494)) ([63355e9](https://github.com/kakao/varlog/commit/63355e9a72351beff51301291bc13ef389576cda))
* **benchmark:** share a connection between appenders in a target ([7dc53e9](https://github.com/kakao/varlog/commit/7dc53e942513feec279c3beccb30b20602d2cb7f))
* **benchmark:** share a connection between appenders in a target ([#524](https://github.com/kakao/varlog/issues/524)) ([2cd9196](https://github.com/kakao/varlog/commit/2cd9196771681a89e036e5bc3b3394a748656b7c))
* **client:** add Clear to the log stream appender manager ([9a89065](https://github.com/kakao/varlog/commit/9a890654c70e6168ae34e8b5321edb7735649110))
* **client:** add Clear to the log stream appender manager ([#514](https://github.com/kakao/varlog/issues/514)) ([e5b6a2e](https://github.com/kakao/varlog/commit/e5b6a2e49866fb9355923746fe404d7c11990b9a))
* **storagenode:** add --storage-trim-delay to set a delay before the deletion of log entries ([db39713](https://github.com/kakao/varlog/commit/db39713654ec962149fa9a1637bf1f38317d0746))
* **storagenode:** add --storage-trim-delay to set a delay before the deletion of log entries ([#529](https://github.com/kakao/varlog/issues/529)) ([015bfa4](https://github.com/kakao/varlog/commit/015bfa4d58164a8ac94686aa68222956494b5838))
* **storagenode:** add --storage-trim-rate to set throttling rate of Trim ([83b7496](https://github.com/kakao/varlog/commit/83b7496cee04303b2c2620cd51558315b2864969))
* **storagenode:** add --storage-trim-rate to set throttling rate of Trim ([#530](https://github.com/kakao/varlog/issues/530)) ([6e69306](https://github.com/kakao/varlog/commit/6e69306b9145851a1d513d25f18afd380a9f6e9f))
* **telemetry:** customize bucket size of process.runtime.go.gc.pause_ns ([b181132](https://github.com/kakao/varlog/commit/b181132df575100f8a2275856b862bd99ae09814))
* **telemetry:** customize bucket size of process.runtime.go.gc.pause_ns ([#510](https://github.com/kakao/varlog/issues/510)) ([9d99520](https://github.com/kakao/varlog/commit/9d9952077a7bfbce5aaeaf4fd1204e418f43acfe))
* **telemetry:** customize bucket size of rpc.server.duration ([a0e5973](https://github.com/kakao/varlog/commit/a0e5973ac9d7f5edc5c370c14bb1c495e0af003b))
* **telemetry:** customize bucket size of rpc.server.duration ([#511](https://github.com/kakao/varlog/issues/511)) ([e41fe1c](https://github.com/kakao/varlog/commit/e41fe1cd4f10670721d8b8be5beae9c5f70927e4))


### Bug Fixes

* **benchmark:** make append duration's precision high ([e3a091d](https://github.com/kakao/varlog/commit/e3a091df53d8308b11c7917f6fa2f8d22dc31d10))
* **benchmark:** make append duration's precision high ([#522](https://github.com/kakao/varlog/issues/522)) ([815af53](https://github.com/kakao/varlog/commit/815af531c9f42520780091ebce10888b1a7eff97))
* **benchmark:** support graceful stop ([8616d55](https://github.com/kakao/varlog/commit/8616d55681ec8a09adbf69e073b76d9e2198da65))
* **benchmark:** support graceful stop ([#527](https://github.com/kakao/varlog/issues/527)) ([fc4ed81](https://github.com/kakao/varlog/commit/fc4ed8165d70ab01c8b02a53007def8dfda0d293))
* **metarepos:** add TestMRIgnoreDirtyReport ([fe2a550](https://github.com/kakao/varlog/commit/fe2a550df1f5917218104d70a2680bba8f380a99))
* **metarepos:** allow set commitTick ([bdca20a](https://github.com/kakao/varlog/commit/bdca20a06b66b21c806eb2ce2022f6d5ad8b5d29))
* **metarepos:** ignore invalid report ([e8620de](https://github.com/kakao/varlog/commit/e8620de689d3eff7670e991500adf242576ae6e1))
* **storagenode:** ignore context error while checking to interleave of Append RPC errors ([04d1052](https://github.com/kakao/varlog/commit/04d1052955959658524e2716bf34294d919b942e))
* **storagenode:** ignore context error while checking to interleave of Append RPC errors ([#504](https://github.com/kakao/varlog/issues/504)) ([5a7a3b0](https://github.com/kakao/varlog/commit/5a7a3b01bdc007da9d7aac15a080f9ef8e613e0a))
* **storagenode:** restore uncommitted logs ([267cccc](https://github.com/kakao/varlog/commit/267ccccc15ff937ab099991c1b61d868c86252d6)), closes [#490](https://github.com/kakao/varlog/issues/490)
* **storagenode:** restore uncommitted logs ([#492](https://github.com/kakao/varlog/issues/492)) ([a9832ee](https://github.com/kakao/varlog/commit/a9832eee6c2532cc29051382a01262db1131595c)), closes [#490](https://github.com/kakao/varlog/issues/490)


### Performance Improvements

* **admin:** use singleflight to handle Admin's RPCs ([c231888](https://github.com/kakao/varlog/commit/c23188830b65885293f124a04cac26df4fb8da81))
* **admin:** use singleflight to handle Admin's RPCs ([#482](https://github.com/kakao/varlog/issues/482)) ([1a6a96d](https://github.com/kakao/varlog/commit/1a6a96da9983bab4305bc1bcd7934c5a9b3e8a24))
* **metarepos:** add a pool for []*mrpb.Report ([fa8c89d](https://github.com/kakao/varlog/commit/fa8c89d8ec6786f8e1b7dc795b03c81bb8589f33))
* **metarepos:** add a pool for []*mrpb.Report ([#534](https://github.com/kakao/varlog/issues/534)) ([16b2181](https://github.com/kakao/varlog/commit/16b2181a2a4d36b48422b00025e0429e90b79383))
* **metarepos:** add a pool for *mrpb.RaftEntry ([be9f121](https://github.com/kakao/varlog/commit/be9f1216175ccc62cfa974e8857b68131d48ce40))
* **metarepos:** add a pool for *mrpb.RaftEntry ([#536](https://github.com/kakao/varlog/issues/536)) ([96ab5e2](https://github.com/kakao/varlog/commit/96ab5e2b482d96ca240f8ab180ea4b8584685bcd))
* **metarepos:** add a pool for mrpb.Reports ([59a6a5a](https://github.com/kakao/varlog/commit/59a6a5afa02058f247f77dcbf8b7d8c37da25f58))
* **metarepos:** add a pool for mrpb.Reports ([#533](https://github.com/kakao/varlog/issues/533)) ([b227c75](https://github.com/kakao/varlog/commit/b227c7570e06084cbd8824fa247794d923426067))
* **metarepos:** avoid copy overhead by removing unnecessary converting from byte slice to string ([a775628](https://github.com/kakao/varlog/commit/a77562854ef687f1a8b25c44407e6fb0aae49659))
* **metarepos:** avoid copy overhead by removing unnecessary converting from byte slice to string ([#532](https://github.com/kakao/varlog/issues/532)) ([1702769](https://github.com/kakao/varlog/commit/1702769fbd5e3aaaadb1a16a73b3ec50f4867b5f))
* **metarepos:** reuse mrpb.StorageNodeUncommitReport while changed ([57d8039](https://github.com/kakao/varlog/commit/57d8039e1814cde8c2147906a93ee516ed923f20))
* **metarepos:** reuse mrpb.StorageNodeUncommitReport while changed ([#537](https://github.com/kakao/varlog/issues/537)) ([8f6e097](https://github.com/kakao/varlog/commit/8f6e097e337a76d2d3aff78e9348b1cbc84069ff))

## [0.14.1](https://github.com/kakao/varlog/compare/v0.14.0...v0.14.1) (2023-06-20)


### Bug Fixes

* **client:** add missing method pkg/varlog.(*MockLog).AppendableLogStreams ([7bf9bf9](https://github.com/kakao/varlog/commit/7bf9bf91660f1361c78976716e9570884f5e86e0))
* **client:** add missing method pkg/varlog.(*MockLog).AppendableLogStreams ([#487](https://github.com/kakao/varlog/issues/487)) ([61747ed](https://github.com/kakao/varlog/commit/61747ed0077d6f7aca9c3a220f0f488bfb154bb8))

## [0.14.0](https://github.com/kakao/varlog/compare/v0.13.0...v0.14.0) (2023-06-19)


### Features

* **client:** add call timeout to log stream appender ([6f916d4](https://github.com/kakao/varlog/commit/6f916d4272ac9ba779e15f71e76e8e2e521ae9f8))
* **client:** add call timeout to log stream appender ([#474](https://github.com/kakao/varlog/issues/474)) ([6db401a](https://github.com/kakao/varlog/commit/6db401a59fcadbe37a0bb1cff7da779f6a266b0f))
* **client:** add LogStreamAppender ([dec3421](https://github.com/kakao/varlog/commit/dec3421ddf305ec161e274643089a5dcfb415b53))
* **client:** add LogStreamAppender ([#459](https://github.com/kakao/varlog/issues/459)) ([bfe88d7](https://github.com/kakao/varlog/commit/bfe88d742e65b3f1265bfa63ef36355b79e4ef6e)), closes [#433](https://github.com/kakao/varlog/issues/433)
* **client:** add settings for gRPC client ([917e5fc](https://github.com/kakao/varlog/commit/917e5fc5231d119ac51b44e6076e4cd0445dcec1))
* **client:** add settings for gRPC client ([#479](https://github.com/kakao/varlog/issues/479)) ([4b8f01c](https://github.com/kakao/varlog/commit/4b8f01c9a50cd0b4770c6a8107c8eb9c5c5707eb))
* **client:** can call Close at callback in LogStreamAppender ([77b8de8](https://github.com/kakao/varlog/commit/77b8de8e041bee5dc9e6a92afe4ab8f653f11205))
* **client:** can call Close at callback in LogStreamAppender ([#473](https://github.com/kakao/varlog/issues/473)) ([23236c1](https://github.com/kakao/varlog/commit/23236c18a7cac87961f7d469989070367dd1cacd))
* **client:** log stream appender manager ([64eae0b](https://github.com/kakao/varlog/commit/64eae0bf9aff698961db7ed27a0fec6ae6923bec))
* **client:** log stream appender manager ([#475](https://github.com/kakao/varlog/issues/475)) ([ac6c7c6](https://github.com/kakao/varlog/commit/ac6c7c6bdc675aeb5881e94bf0015196636b8386))
* **client:** support goroutine-safety of pkg/var log.(LogStream Appender).Append Batch ([7fa53f3](https://github.com/kakao/varlog/commit/7fa53f303e0623dbe966e763dfbfff263f6fddba))
* **client:** support goroutine-safety of pkg/var log.(LogStream Appender).Append Batch ([#472](https://github.com/kakao/varlog/issues/472)) ([46f0d0b](https://github.com/kakao/varlog/commit/46f0d0b8179e4a70c1244e759082014bc8c799a3))
* **storagenode:** add configurations for initial window size ([8b623b0](https://github.com/kakao/varlog/commit/8b623b012a95123b393ed85f69cee0df0deae0c9))
* **storagenode:** add configurations for initial window size ([#451](https://github.com/kakao/varlog/issues/451)) ([5269481](https://github.com/kakao/varlog/commit/5269481c0e80c2eebf8214116a2d1544a26cb443))
* **storagenode:** add pipelined Append RPC handler ([8535a4d](https://github.com/kakao/varlog/commit/8535a4daa4d7f749ad8601a455a5f6cda1344800))
* **storagenode:** add pipelined Append RPC handler ([#457](https://github.com/kakao/varlog/issues/457)) ([0323629](https://github.com/kakao/varlog/commit/0323629d50d547e6312d01e3dcf6e2b0d7324b30))
* **storagenode:** change append rpc from unary to stream ([5b27a18](https://github.com/kakao/varlog/commit/5b27a186988a725acfe5880db910f61169aa3aea))
* **storagenode:** change append rpc from unary to stream ([#449](https://github.com/kakao/varlog/issues/449)) ([7e4ef03](https://github.com/kakao/varlog/commit/7e4ef0351ed3cc74d4e11f9ce18bf2dcbf9444d2))


### Bug Fixes

* **storagenode:** accept SyncInit sent from trimmed source to new destination ([0e141bd](https://github.com/kakao/varlog/commit/0e141bdc1e180dfa8b877f3de083e1c9efe29cd0)), closes [#478](https://github.com/kakao/varlog/issues/478)
* **storagenode:** support partial success/failure for append ([5d438bb](https://github.com/kakao/varlog/commit/5d438bb111cb6bfd7af69821200420afcdbbd151))
* **storagenode:** support partial success/failure for append ([#450](https://github.com/kakao/varlog/issues/450)) ([c8b7fe0](https://github.com/kakao/varlog/commit/c8b7fe07c346295e81c3b1639d432d4000ecd6b0))

## [0.13.0](https://github.com/kakao/varlog/compare/v0.12.0...v0.13.0) (2023-06-01)


### Features

* **admin:** add a new flag `replica-selector` to the varlogadm ([805f8de](https://github.com/kakao/varlog/commit/805f8de5ad7f20cf3153c81afd04a41fe70ff67c)), closes [#393](https://github.com/kakao/varlog/issues/393)
* **all:** add flags for logger ([7efe407](https://github.com/kakao/varlog/commit/7efe407f2e85d2ac6271873d7424ec0f5d28e857)), closes [#439](https://github.com/kakao/varlog/issues/439)
* **all:** add flags for logger ([#447](https://github.com/kakao/varlog/issues/447)) ([f2e1193](https://github.com/kakao/varlog/commit/f2e1193d453a5f724d85e4d3c8aa3ad1aa3287d8)), closes [#439](https://github.com/kakao/varlog/issues/439)
* **storage:** separate storage databases experimentally ([b3845f5](https://github.com/kakao/varlog/commit/b3845f568fb6f39b284d38935e991f8d5df3c4ac))
* **storage:** separate storage databases experimentally ([#410](https://github.com/kakao/varlog/issues/410)) ([9f64785](https://github.com/kakao/varlog/commit/9f64785de64b4bdc8ecb67ee746bdd36cfbf52e3))


### Bug Fixes

* **metarepos:** new topic should start from MinGLSN ([d1ae8c8](https://github.com/kakao/varlog/commit/d1ae8c808f69a7c2c4b425c1dc98ceffc153ffd6))


### Performance Improvements

* **metarepos:** add mrpb.StorageNodeUncommitReport pool ([4c624da](https://github.com/kakao/varlog/commit/4c624dabce5ec063c797df92ed44930178b67d2b))
* **metarepos:** add mrpb.StorageNodeUncommitReport pool ([#446](https://github.com/kakao/varlog/issues/446)) ([60f2cfe](https://github.com/kakao/varlog/commit/60f2cfe96d4cc574ab933358217811635f3649be))
* **storagenode:** check log level ([39cdbae](https://github.com/kakao/varlog/commit/39cdbaeb1b2b8412af6f8b80853d6fe67f45f887))
* **storagenode:** check log level ([#411](https://github.com/kakao/varlog/issues/411)) ([da1409d](https://github.com/kakao/varlog/commit/da1409d1d555351eca099bbf1c507d2c2b0d583a))
* **storagenode:** estimate the number of batchlets ([3c91b62](https://github.com/kakao/varlog/commit/3c91b6214a058fa5850885d13f550a23c2ddde8c))
* **storagenode:** estimate the number of batchlets ([#414](https://github.com/kakao/varlog/issues/414)) ([cb25d19](https://github.com/kakao/varlog/commit/cb25d195cbac2a25c58c0ccb0d5aa34d383241f3))
* **storagenode:** remove backup from append request ([f75ef55](https://github.com/kakao/varlog/commit/f75ef55b2dcf5098b35750660691c9893a088446))
* **storagenode:** remove backup from append request ([#412](https://github.com/kakao/varlog/issues/412)) ([0abcb1a](https://github.com/kakao/varlog/commit/0abcb1a029036f20d59b2c5038b68632a4ecdbf8))
* **storagenode:** wrap replicateTask slice with struct ([37250e1](https://github.com/kakao/varlog/commit/37250e1670831baa7d472e3c4832c4f21701944c)), closes [#75](https://github.com/kakao/varlog/issues/75)
* **storagenode:** wrap replicateTask slice with struct ([#416](https://github.com/kakao/varlog/issues/416)) ([911b5fe](https://github.com/kakao/varlog/commit/911b5fe7d298aaa5a4ca74b9501a2778b49f4c77)), closes [#75](https://github.com/kakao/varlog/issues/75)

## [0.12.0](https://github.com/kakao/varlog/compare/v0.11.0...v0.12.0) (2023-04-06)


### Features

* **admin:** add a name to the replica selector ([6e7e74f](https://github.com/kakao/varlog/commit/6e7e74fda80693a86f8c45819ba9616c3dbad7ef)), closes [#393](https://github.com/kakao/varlog/issues/393)
* **admin:** LFU replica selector ([0e08901](https://github.com/kakao/varlog/commit/0e08901189896d7dae74d694d78a82768cf130d9)), closes [#393](https://github.com/kakao/varlog/issues/393)
* **admin:** random replica selector ([ad1226e](https://github.com/kakao/varlog/commit/ad1226e031fb5b90a6a2634901e84efbe8f84a6b)), closes [#393](https://github.com/kakao/varlog/issues/393)
* **storage:** metrics logging and less verbose event logging ([3b47f8b](https://github.com/kakao/varlog/commit/3b47f8be0a34c57f931c1133853c7b6cb6811ab9))
* **storagenode:** add automaxprocs to storagenode ([64f2afe](https://github.com/kakao/varlog/commit/64f2afef925a570831e6823ed0c3754cc77ed975))
* **storagenode:** change SyncInit to handle trimmed source replica ([79c5323](https://github.com/kakao/varlog/commit/79c5323f0656cf7c17077d7ed3f98b5ddebb4f34)), closes [#351](https://github.com/kakao/varlog/issues/351)
* **storagenode:** restore trimmed log stream replica ([02106fa](https://github.com/kakao/varlog/commit/02106fae07af76c132c0d3fadbf83557a3b369c8)), closes [#351](https://github.com/kakao/varlog/issues/351)
* **storagenode:** trim API with no safety gap ([2fe4a8e](https://github.com/kakao/varlog/commit/2fe4a8e01b9cf1211cacd95a167cad819087e656)), closes [#351](https://github.com/kakao/varlog/issues/351)


### Bug Fixes

* **metarepos:** fix race in TestReportCommit ([ff21f66](https://github.com/kakao/varlog/commit/ff21f66ab5b6eb63267a16311fabcf73bb18603b)), closes [#379](https://github.com/kakao/varlog/issues/379)
* **storagenode:** configure ballast by using flags ([cc38fcc](https://github.com/kakao/varlog/commit/cc38fccab50a58defef6f756d8da3ee7fbe09c55))
* **storagenode:** set the flag --storage-mem-table-size correctly ([872d796](https://github.com/kakao/varlog/commit/872d79699bae3533565108819a81cbfc87709010))
* **storagenode:** subscribe empty range below the global high watermark ([1ef39f8](https://github.com/kakao/varlog/commit/1ef39f8adfa19c90b467b357b69979b478425e29)), closes [#375](https://github.com/kakao/varlog/issues/375)
* **storage:** prints storage log clearly ([6e868be](https://github.com/kakao/varlog/commit/6e868be273d581e7910da29b5ffe655b1dbb3c04))


### Performance Improvements

* **storage:** change storage key prefixes ([8d49523](https://github.com/kakao/varlog/commit/8d4952300bb80627b3270e8daccd69bc29ac3f69))

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
