# Varlog 정의 
Varlog는 strongly-consistency와 total-order를 제공하는 분산 로그 스토리지이다. Immutable log entry가 append되고, 이를 read하는 단순한 API를 통해 분산 환경에서 제공하기 어려운 문제들을 해결한다. 예를 들면, 분산 노드들에서 발생한 이벤트들을 차례대로 저장할 수 있으며, 모든 노드들이 일관성있게 읽을 수 있다. 이를 통해 State Machine Replication (SMR)과 Transaction Log, Distributed WAL (Write Ahead Log) 등을 제공한다.

# Varlog 구성

Varlog는 Client, Storage Node, Metadata Repository 로 구성된다. Storage Node는 Log Entry를 저장하며, Metadata Repository는 Global Log의 total order를 생성한다. Client는 Read, Subscribe, Append, Trim 등 단순한 API를 통해 로그를 읽고 쓴다.


                            +-------------+
                            | StorageNode |
                            +-------------+
                            | StorageNode |            +-------------+
                            +-------------+            |  Metadata   |
                            | StorageNode |            | Repository  |
                            +-------------+            +-------------+
    +-------------+         | StorageNode |            |  Metadata   |
    |             |         +-------------+  \      /  | Repository  |
    |   Client    | <-----> | StorageNode |  -----|--  +-------------+
    |             |         +-------------+  /      \  |      .      |
    +-------------+         | StorageNode |            |      .      |
                            +-------------+            |      .      |
                            | StorageNode |            +-------------+
                            +-------------+            |  Metadata   |
                            | StorageNode |            | Repository  |
                            +-------------+            +-------------+
                            |      .      |
                            |      .      |
                            |      .      |
                            +-------------+
                            | StorageNode |
                            +-------------+

