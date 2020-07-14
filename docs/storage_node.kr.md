# Storage Node

Storage Node는 Client의 요청을 받아 Log Entry를 읽고 쓴다. 그리고 Metadata Repository의 Report 요청과 Commit 결과 전달을 처리한다. Storage Node는 아래와 같이 구성된다.

- RPC Services
- Log Stream Reporter
- Log Stream Executors
- Storages
- Replicators

RPC Service는 제공하는 기능에 따라 ~~Storage Node Service~~Log I/O Service, Storage Node Admin Service, Replicator Service, Log Stream Reporter Service 등으로 구성된다. Log Stream Service는 Metadata Repository 로부터 Report 요청과 Commit 결과 전달을 핸들링하고 Log Stream Reporter를 호출한다. Log Stream Reporter는 Log Stream Executor 에게 Report 를 받아 Metadata Repository에게 전달하거나 Metadata Repository로부터 전달받은 Commit 결과를 Log Stream Executor에게 알려준다. Log Stream Executor는 Storage Node의 핵심 모듈로써 Client로 부터 받은 Log I/O 요청을 수행하거나 Log Stream Executor에게 요청 받은 Report 전달 및 Commit 처리를 수행한다. 그 외의 서비스들은 나중에 설명을 하도록 한다.


       +--MetadataRepository------------+
       |                                |
       |  +--------------------------+  |
    +->|  | LogStreamReporterClient  |  |
    |  |  +--------------------------+  |
    |  +--------------------------------+
    |
    |          +--StorageNode---------------------------------------------------------------------------------+
    |          |                                                                                              |
    |          |                                       +---------------------------------------------------+  |
    |          |                                       |                                                   |  |
    |          |                                       |                 LogStreamExecutor                 |  |
    |          |                                       |                                                   |  |
    |          |                                       |  +--------------+ +----------------------------+  |  |
    |          |                                       |  |              | |         Replicator         |  |  |
    |          |  +----------------------------+       |  |              | |                            |  |  |
    |          |  |        RPC Services        |       |  |              | |      +------------------+  |  |  |
    |          |  |                            |  +--->|  |   Storage    | |    +-+----------------+ |  |  |  |
    |          |  | +------------------------+ |  |    |  |              | |  +-+----------------+ | |  |  |  |
    |          |  | |   ManagementService    | |  |    |  |              | |  | ReplicatorClient | +-+  |  |  |
    |          |  | +------------------------+ |  |    |  |              | |  |                  +-+    |  |  |
    |          |  | |      LogIOService      | |  |    |  |              | |  +------------------+      |  |  |
    |          |  | +------------------------+ |  |    |  +--------------+ +----------------------------+  |  |
    |          |  | |   ReplicatorService    | |  |    |                                                   |  |
    |  report  |  | +------------------------+ |  |    +---------------------------------------------------+  |
    +--commit--+--+>|LogStreamReporterService| |  |    +---------------------------------------------------+  |
               |  | +------------------------+ |  |    |                                                   |  |
               |  |              ^             |  |    |                 LogStreamExecutor                 |  |
               |  +--------------|-------------+  |    |                                                   |  |
               |                 v                |    |  +--------------+ +----------------------------+  |  |
               |    +------------------------+    |    |  |              | |         Replicator         |  |  |
               |    |   LogStreamReporter    |----+    |  |              | |                            |  |  |
               |    +------------------------+    |    |  |              | |      +------------------+  |  |  |
               |                                  +--->|  |   Storage    | |    +-+----------------+ |  |  |  |
               |                                       |  |              | |  +-+----------------+ | |  |  |  |
               |                                       |  |              | |  | ReplicatorClient | +-+  |  |  |
               |                                       |  |              | |  |                  +-+----+--+--+--+
               |                                       |  |              | |  +------------------+      |  |  |  |
               |                                       |  +--------------+ +----------------------------+  |  |  |
               |                                       |                                                   |  |  |
               |                                       +---------------------------------------------------+  |  |
               |                                                                                              |  |
               +----------------------------------------------------------------------------------------------+  |
                                                                                                        replication
            +----------------------------------------------------------------------------------------------------+
            |
            |  +--StorageNode---------------------------------------------------------------------------------+
            |  |                                                                                              |
            |  |                                       +---------------------------------------------------+  |
            |  |                                       |                                                   |  |
            |  |                                       |                 LogStreamExecutor                 |  |
            |  |                                       |                                                   |  |
            |  |                                       |  +--------------+ +----------------------------+  |  |
            |  |                                       |  |              | |         Replicator         |  |  |
            |  |  +----------------------------+       |  |              | |                            |  |  |
            |  |  |        RPC Services        |       |  |              | |      +------------------+  |  |  |
            |  |  |                            |  +--->|  |   Storage    | |    +-+----------------+ |  |  |  |
            |  |  | +------------------------+ |  |    |  |              | |  +-+----------------+ | |  |  |  |
            |  |  | |   ManagementService    | |  |    |  |              | |  | ReplicatorClient | +-+  |  |  |
            |  |  | +------------------------+ |  |    |  |              | |  |                  +-+    |  |  |
            |  |  | |      LogIOService      | |  |    |  |              | |  +------------------+      |  |  |
            |  |  | +------------------------+ |  |    |  +--------------+ +----------------------------+  |  |
            +--+--+>|   ReplicatorService    | |  |    |                                                   |  |
               |  | +------------------------+ |  |    +---------------------------------------------------+  |
               |  | |LogStreamReporterService| |  |    +---------------------------------------------------+  |
               |  | +------------------------+ |  |    |                                                   |  |
               |  |              ^             |  |    |                 LogStreamExecutor                 |  |
               |  +--------------|-------------+  |    |                                                   |  |
               |                 v                |    |  +--------------+ +----------------------------+  |  |
               |    +------------------------+    |    |  |              | |         Replicator         |  |  |
               |    |   LogStreamReporter    |----+    |  |              | |                            |  |  |
               |    +------------------------+    |    |  |              | |      +------------------+  |  |  |
               |                                  +--->|  |   Storage    | |    +-+----------------+ |  |  |  |
               |                                       |  |              | |  +-+----------------+ | |  |  |  |
               |                                       |  |              | |  | ReplicatorClient | +-+  |  |  |
               |                                       |  |              | |  |                  +-+    |  |  |
               |                                       |  |              | |  +------------------+      |  |  |
               |                                       |  +--------------+ +----------------------------+  |  |
               |                                       |                                                   |  |
               |                                       +---------------------------------------------------+  |
               |                                                                                              |
               +----------------------------------------------------------------------------------------------+


## RPC Services

### Log I/O Service

Log I/O Service는 Client로 부터 받은 Read, Subscribe, Append, Trim 요청을 핸들링한다. Read 요청과 Append 요청은 특정 Log Stream을 요청 파라미터로 받는다. 해당 Log Stream을 서비스하는 Log Stream Executor에게 이를 전달한다. 반면에 Subscribe와 Trim은 Storage Node가 갖고 있는 모든 Log Stream Executor에게 전달한다.

### Replicator Service

Replicator Service는 Log Entry 복제에 대한 요청을 핸들링한다. 이 RPC의 호출자는 Primary 역할을 하는 Log Stream Executor 이다 (구체적으로 Primary Log Stream Executor가 갖고 있는 Replicator가 관리하는 Replicator Client).

### Log Stream Reporter Service

Log Stream Reporter Service는 Metadata Repository에게 Storage Node에 있는 Log Stream들의 상태 정보 (uncommitted logs, ...)를 전달하고, 이를 바탕으로 Metadata Repository의 Global Commit 수행 결과를 수신한다.

### Storage Node Admin Service

Storage Node를 관리하는 RPC 호출을 핸들링한다. Log Stream을 생성, Seal, Unseal, Recovery 등을 수행하거나 Storage Node가 varlog 클러스터에 참가 요청을 수신한다.

## Log Stream Reporter

Log Stream Reporter는 Storage Node가 호스팅하는 Log Stream들의 정보를 수집하여 Metadata Repository에게 전달한다. Log Stream Executor가 제공하는 정보 중 가장 주요한 것은 uncommitted local log이다. 즉, 각 Log Stream이 Client로부터 혹은 Primary로부터 받아서 Storage 에 write했지만, GLSN (Global Log Sequence Number)를 할당받지 못하여 durable 한 상태가 아닌 Log Entry들에 대한 정보이다. Metadata Repository는 클러스터 내의 모든 Log Stream들로부터 이 정보를 수신 받아 Global Log 공간 내의 Commit을 수행한다. Commit 결과는 각 Storage Node의 Log Stream Reporter Service에게 다시 전달되고, 이는 Log Stream Reporter에 의해 처리된다.

## Log Stream Executor

Log Stream Executor는 Log Stream에 대한 모든 처리를 수행한다. Log 에 대한 I/O는 Storage를 사용하여 처리하고, Primary Log Stream Executor는 Replicator를 사용하여 Log Entry를 Backup Log Stream Executor들에게 전달한다.

### Storage

Storage는 단순한 인터페이스를 제공하며, fault의 가능성이 적어야 한다. Storage가 제공하는 API는 Read, Write, Commit, Scan, Delete 이다. Storage는 Log Stream이 관리하는 uncommitted, committed logs에 대한 정보, commited GLSN 정보, trim 된 log range등의 정보를 알필요 없이 Log Stream Executor의 요청에 따라 I/O 를 수행하면 된다.

Storage에 대한 설계는 나중에 다른 문서로 더 자세히 다룬다.

### Replicator

Varlog의 Replication 방식은 Primary/Backup을 사용한다. Primary Log Stream Executor는 Replicator를 사용하여 Backup에 Log Entry를 복제한다. 이때 Log가 Primary에 쓰여진 순서대로 Backup에 쓰여지도록 해야한다. 만약 Primary와 Backup에 같은 Log Entry의 복제본이 서로 다른 순서로 저장된다면 consistency를 보장할 수 없다. Replicator는 Log Entry의 복제를 수행하며 Replication 실패 시 Log Stream Executor에게 알려준다.

# Log I/O Client ~~Storage Node Client~~

Client 라이브러리가 Storage Node에 Log Entry를 읽고 쓰거나 지우기 위해서는 Log I/O Client 를 사용한다. Log I/O Client는 Client 라이브러리 사용자가 알 필요는 없다. 그저 Client 라이브러리를 구현하기 위해서 필요한 모듈이다.


    +----------------------+
    |Varlog Client Library |
    |                      |
    | +------------------+ |         +----------------------+
    | |   LogIOClient    | |         |     LogIOService     |
    | |                  | |         |                      |
    | | +--------------+ | |         | +------------------+ |
    | | |     RPC      |<+-+---------+>|       RPC        | |
    | | +--------------+ | |         | +------------------+ |
    | +------------------+ |         +----------------------+
    | +------------------+ |         +----------------------+
    | |   LogIOClient    | |         |     LogIOService     |
    | |                  | |         |                      |
    | | +--------------+ | |         | +------------------+ |
    | | |     RPC      |<+-+---------+>|       RPC        | |
    | | +--------------+ | |         | +------------------+ |
    | +------------------+ |         +----------------------+
    |          .           |                    .
    |          .           |                    .
    |          .           |                    .
    | +------------------+ |         +----------------------+
    | |   LogIOClient    | |         |     LogIOService     |
    | |                  | |         |                      |
    | | +--------------+ | |         | +------------------+ |
    | | |     RPC      |<+-+---------+>|       RPC        | |
    | | +--------------+ | |         | +------------------+ |
    | +------------------+ |         +----------------------+
    +----------------------+


