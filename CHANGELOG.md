# Changelog

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
