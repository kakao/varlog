# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Added `CHANGELOG.md`. (#VARLOG-631/#548)
- Added `mrtool` to retrieve metadata of MR without VMS. (#VARLOG-612/#546)
- Added a new k8s manifests - `deploy/k8s-experiment` that changes MR from DaemonSet to StatefulSet. (#VARLOG-612/#546)
- Added a new MR launcher - `bin/start_varlogmr.py` that uses `mrtool`. (#VARLOG-612/#546)

### Removed
- Remove unnecessary file `TOPIC`. (#VARLOG-631/#548)


## [0.1.0] - 2021-12-15

[Unreleased]: https://github.com/kakao/varlog/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kakao/varlog/releases/tag/v0.1.0
