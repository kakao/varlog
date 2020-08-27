- Status:
- Authors: jun.song
- Reviewer: pharrell.jang
- Date: 2020-07



## Management in StorageNode

storagenode 프로세스를 실행하여 새로운 StorageNode를 시작한다. 처음 실행된 StorageNode는 다음 operation들을 통해 관리된다.

- AddLogStream: LogStream 읽고 쓰기 위한 자료 구조 및 고루틴을 생성한다.
- RemoveLogStream: LogStream 읽고 쓰기 위한 자료 구조를 삭제하고 고루틴을 종료한다.
- Seal: LogStream이 Append 요청을 처리할 수 없도록 상태를 변경한다.
- Unseal: Seal된 LogStream이 다시 Append 요청을 처리할 수 있도록 상태를 변경한다.
- Sync: 같은 LogStream을 서빙하는 두 개의 LogStream Replica 간의 로그 데이터 미러링을 한다.

### State diagram of LogStreamExecutor

LSE는 시작되면 Running 상태이다. 만약 LS에 문제가 생기면 스스로 Seal하거나 MCL에 의해 Seal되어진다 (SEALING). MCL은 LS의 장애를 감지하고 Seal RPC를 호출하는데, 이때 MR이 알고 있는 해당 LS의 마지막 Commit Log 위치 (Last Committed LSN)를 파라미터로 보낸다. LS는 Last Committed LSN과 자신이 로컬에 Commit한 로그 위치를 비교하여 서로 같다면 SEALED로 상태를 변경한다. SEALING과 SEALED 상태에서는 Append 요청을 수행할 수 없으며 Last Committed LSN보다 큰 위치의 로그를 Commit하지 않는다. 만약 MR로 부터 Last Committed LSN보다 큰 로그 위치의 Commit 요청이 왔다면, Varlog 클러스터의 data consistency에 문제가 발생했다는 의미이다.

[![](https://mermaid.ink/img/eyJjb2RlIjoic3RhdGVEaWFncmFtLXYyXG5cblsqXSAtLT4gUlVOTklOR1xuUlVOTklORyAtLT4gU0VBTElORzogU2VhbCBpdHNlbGYgb3IgU2VhbCAoQ29tbWl0dGVkIExTTiA8IExhc3QgQ29tbWl0dGVkIExTTilcblJVTk5JTkcgLS0-IFNFQUxFRDogU2VhbCAoQ29tbWl0dGVkIExTTiA9IExhc3QgQ29tbWl0dGVkIExTTilcblNFQUxJTkcgLS0-IFNFQUxFRDogU2VhbCAoQ29tbWl0dGVkIExTTiA9IExhc3QgQ29tbWl0dGVkIExTTilcblNFQUxFRCAtLT4gUlVOTklORzogVW5zZWFsIiwibWVybWFpZCI6eyJ0aGVtZSI6ImRlZmF1bHQifSwidXBkYXRlRWRpdG9yIjpmYWxzZX0)](https://mermaid-js.github.io/mermaid-live-editor/#/edit/eyJjb2RlIjoic3RhdGVEaWFncmFtLXYyXG5cblsqXSAtLT4gUlVOTklOR1xuUlVOTklORyAtLT4gU0VBTElORzogU2VhbCBpdHNlbGYgb3IgU2VhbCAoQ29tbWl0dGVkIExTTiA8IExhc3QgQ29tbWl0dGVkIExTTilcblJVTk5JTkcgLS0-IFNFQUxFRDogU2VhbCAoQ29tbWl0dGVkIExTTiA9IExhc3QgQ29tbWl0dGVkIExTTilcblNFQUxJTkcgLS0-IFNFQUxFRDogU2VhbCAoQ29tbWl0dGVkIExTTiA9IExhc3QgQ29tbWl0dGVkIExTTilcblNFQUxFRCAtLT4gUlVOTklORzogVW5zZWFsIiwibWVybWFpZCI6eyJ0aGVtZSI6ImRlZmF1bHQifSwidXBkYXRlRWRpdG9yIjpmYWxzZX0)

### Sync

MR이 Confirm한 모든 로그를 저장한 LogStream Replica A1 와 새롭게 추가되어 데이터를 갖고 있는 않은 LogStream Replica A2 가 같은 LogStream A 를 서빙한다 가정한다. LogStream A가 다시 Unseal 되어 새로운 LogEntry를 저장하기 위해서는 먼저 A2에 데이터가 복제되어야 한다. 이를 위해 A1에 Sync RPC를 호출한다.
`A1.Sync(A2)` 는 A1의 데이터를 A2에 미러링하게 되는데, 미러링할 데이터 양에 따라 이 과정이 오래걸릴 수 있다. MCL은 `A1.Sync(A2)` RPC를 주기적으로 호출하여 미러링 작업 진행 상태 (SYNC_INPROGRESS, SYNC_COMPLETE)를 알 수 있다. 

### Architecture

```
                                      +--------------------------+
                                      |                          |
                                      |          Stats           |<-+
                                      |                          |  |
                                      +--------------------------+  |
                                                    ^               |
                                                    |               |
                                                    v               |
    +-----------------------+         +--------------------------+  |
    |   ManagementClient    |---RPC-->|    ManagementService     |  |
    +-----------------------+         +--------------------------+  |
                                                    |               |
                                                    |               |
                      +-----------------------------+               |
                      |                                             |
                      |              +----------LSEMap-----------+  |
                      v              |                           |  |
               +-------------+       | +-----------------------+ |  |
               |             |       | |   LogStreamExecutor   | |  |
               | StorageNode |----+  | +-----------------------+ |  |
               |             |    |  | +-----------------------+ |  |
               +-------------+    |  | |   LogStreamExecutor   | |  |
                                  |  | +-----------------------+ |  |
                                  |  | +-----------------------+ |<-+
                                  |  | |   LogStreamExecutor   | |
                                  |  | +-----------------------+ |
                                  |  | +-----------------------+ |
                                  +--+>|   LogStreamExecutor   | |
                                     | +-----------------------+ |
                                     |                           |
                                     +---------------------------+
```

