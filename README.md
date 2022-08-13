# Varlog 

[![ci](https://github.com/kakao/varlog/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/kakao/varlog/actions?query=workflow%3Aci+branch%3Amain)
[![codecov](https://codecov.io/gh/kakao/varlog/branch/main/graph/badge.svg?token=E6ULNVKEZM)](https://codecov.io/gh/kakao/varlog)


Varlog is a strongly consistent distributed log storage. It enables many
distributed systems to leverage total-ordered logs to support transactional
event processing with high performance, and simplify the building the complex
distributed systems.

## Installation

With Go module support, add the following import to your code.

```go
import "github.com/kakao/varlog"
```

## Usage

```go
// Open the log.
vlog, _ := varlog.Open(context.Background(), clusterID, mrAddrs)

// Write records to the log.
vlog.Append(context.Background(), topicID, [][]byte{[]byte("hello"), []byte("varlog")})

// Read records from the log.
closer, _ := vlog.Subscribe(context.Background(), topicID, begin, end, func(logEntry varlogpb.LogEntry, err error) {
    fmt.Println(logEntry)
})
defer closer()
```

## License

This software is licensed under the [Apache 2 license](LICENSE), quoted below.

Copyright 2022 Kakao Corp. <http://www.kakaocorp.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this project except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
