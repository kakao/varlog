- Authors: jun.song
- Reviewer: pharrell.jang
- Status: 
- Start Date: 2020-05-10

# Storage Node

## Terminology
- SN (Storage Node)
- CL (Client Library)
- LE (Log Entry)
- GLSN (Global Log Sequence Number)
- LS (Log Stream)

각 구성 요소들을 도식화하여 이해하기 쉽도록 아래에 표현했다.  
사용자가 저장하는 데이터를 Log Entry라고 한다. 각 Log Entry는 GLSN (Global Log
Sequence Number)을 갖는다. GLSN은 Log 공간 내에 유일한 위치 값이며, Log Entry를
식별하고 이것을 이용해서 순서를 알 수 있다.
Varlog는 분산된 서버에 Log를 저장하므로, 논리적으로 연속된 로그이지만 실제로는
각 서버에 로그들이 나누어져 저장되어 있다. 각 서버들이 저장하고 있는 로그의
일부분을 Log Stream 이라고 한다. Log Stream 에 속한 Log Entry는 GLSN이
연속적이지는 않지만 증가하는 값을 갖는다. High Availability를 위해 Log Stream은
여러 노드에 중복되어 저장된다.  
아래 그림은 replication factor = 3 (Log Stream을 3개의 복사본으로 관리) 인
설정이다.
                                                    
    +---------------------------------------------------------------------------------------+
    |   +-------+     +-------+      +-------+     +-------+     +-------+      +-------+   |
    |   |       |     |       |      |       |     |       |     |       |      |       |   |
    | +Log Stream-1---+-------+------+-------+-+ +Log Stream-6---+-------+------+-------+-+ |
    | | | +---+ |     | +---+ |      | +---+ | | | | +---+ |     | +---+ |      | +---+ | | |
    | | | | P | |     | | B | |      | | B | | | | | | P | |     | | B | |      | | B | | | |
    | | | +---+ |     | +---+ |      | +---+ | | | | +---+ |     | +---+ |      | +---+ | | |
    | +-+-------+-----+-------+------+-------+-+ +-+-------+-----+-------+------+-------+-+ |
    |   |       |     |       |      |       |     |       |     |       |      |       |   |
    | +Log Stream-2---+-------+------+-------+-+ +Log Stream-5---+-------+------+-------+-+ |
    | | | +---+ |     | +---+ |      | +---+ | | | | +---+ |     | +---+ |      | +---+ | | |
    | | | | B | |     | | P | |      | | B | | | | | | B | |     | | P | |      | | B | | | |
    | | | +---+ |     | +---+ |      | +---+ | | | | +---+ |     | +---+ |      | +---+ | | |--+
    | +-+-------+-----+-------+------+-------+-+ +-+-------+-----+-------+------+-------+-+ |  |
    |   |       |     |       |      |       |     |       |     |       |      |       |   |  |
    | +-+-------+-+ +Log Stream-3----+-------+-----+-------+-+ +Log Stream-4----+-------+-+ |  |
    | | | +---+ | | | | +---+ |      | +---+ |     | +---+ | | | | +---+ |      | +---+ | | |  |
    | | | | B | | | | | | B | |      | | P | |     | | B | | | | | | B | |      | | P | | | |  |
    | | | +---+ | | | | +---+ |      | +---+ |     | +---+ | | | | +---+ |      | +---+ | | |  |
    | +-+-------+-+ +-+-------+------+-------+-----+-------+-+ +-+-------+------+-------+-+ |  |
    |   |       |     |       |      |       |     |       |     |       |      |       |   |  |
    |   +-------+     +-------+      +-------+     +-------+     +-------+      +-------+   |  |
    |    Storage       Storage        Storage       Storage       Storage        Storage    |  |
    |      Node          Node           Node          Node          Node           Node     |  |
    +---------------------------------------------------------------------------------------+  |
                                                                                               |
       Log                                                                                     |
      +---+---+---+---+---+---+---+---+---+---+---+                                            |
      |   |   |   |   |   |   |   |   |   |   |   |                                            |
      | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |10 |11 |     <--------------------------------------+
      |   |   |   |   |   |   |   |   |   |   |   |
      +---+---+---+---+---+---+---+---+---+---+---+
        ^   ^   ^   ^   ^
        |   |   |   |   |
        | +-+   |   | +-+
        | |     |   | |
        | |     |   | |
       ++-++ +--++ ++-++
       | L | | L | | L |
       | o | | o | | o |
       | g | | g | | g |
       |   | |   | |   |
       | S | | S | | S |
       | t | | t | | t |   ...
       | r | | r | | r |
       | e | | e | | e |
       | a | | a | | a |
       | m | | m | | m |
       |   | |   | |   |
       | 1 | | 8 | | 3 |
       +---+ +---+ +---+

## Basic APIs
SN은 CL이 사용할 수 있는 아래 API들을 제공한다: 

- append: LE를 varlog에 저장하고 GLSN과 LS ID를 반환한다.
- subscribe: 요청한 GLSN과 같거나 큰 로그 위치에 저장된 LE들을 계속해서
전달 받는다.
- read: LS ID와 GLSN을 전달하여, 해당 LS에 GLSN 위치값에 저장된 LE가 있다면 
반환한다.

아래는 이해를 돕기 위해 간단한 pseudo code로 API signature를 정리했다.

```
func Append(logEntry LogEntry, opts ...Options(GLSN, LogStreamID) { ... }

func Subscribe(glsn GLSN) LogEntryStream { ... }

func Read(logStream LogStreamID, glsn GLSN) LogEntry { ... }
```

### Append
CL이 append를 시작하면, 임의의 LS를 선택한다. 로그 설정 혹은 append 설정에 의해
특정 LS를 선택할 수도 있다. LS를 구성하는 모든 SN들은 MR에게 local cut (Local
Log Stream Info)를 전달하는데, 이것은 append 요청받은 Log Entry가 durable해지기
위해 GLSN 발급을 요청하는 것이다. 이를 위해 MR은 각 SN에게 저장하고 있는 LS에
대한 정보를 달라고 요청할 것이다.

    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
    |  CL1  |   |  CL2  |   |  SN1  |   |  SN2  |   |  SN3  |   |  SN4  |    |  MR   |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        +--append---+----------->           |           |           |            |
        |           |          ++--LS-1-----+-----------++          |            |
        |           |          |+--copy----->           ||          |            |
        |           |          |+--copy-----+----------->|          |            |
        |           |          ++-----------+-----------++          |            |
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        |           +--append---+----------->           |           |            |
        |           |           |          ++--LS-2-----+-----------++           |
        |           |           |          |+--copy----->           ||           |
        |           |           |          |+--copy-----+----------->|           |
        |           |           |          ++-----------+-----------++           |
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        |           |           +--glsn req-+-----------+-----------+------------>
        |           |           |           +--glsn req-+-----------+------------>
        |           |           |           |           +--glsn req-+------------>
        |           |           |           |           |           +--glsn req-->
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        |           |           <-----------+-----------+-----------+--glsn ack--+
        |           |           |           <-----------+-----------+--glsn ack--+
        <-------ack(glsn,ls-1)--+           |           <-----------+--glsn ack--+
        |           |           |           |           |           <--glsn ack--+
        |           <-------ack(glsn,ls-2)--+           |           |            |
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
    |  CL1  |   |  CL2  |   |  SN1  |   |  SN2  |   |  SN3  |   |  SN4  |    |  MR   |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+


### Subscribe
subscribe는 각 LS를 저장하는 SN들중 하나에게 durable한 LE를 스트림 받도록
요청한다. (혹은 CL이 하나씩 차례대로 next LE를 요청할 수 있다.)

    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
    |  CL1  |   |  CL2  |   |  SN1  |   |  SN2  |   |  SN3  |   |  SN4  |    |  MR   |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        |           |          ++--LS-1-----+-----------++          |            |
        +--subscribe+----------+>           |           ||          |            |
        |           |          ++-----------+-----------++          |            |
        |           |           |          ++--LS-2-----+-----------++           |
        +--subscribe+-----------+----------+>           |           ||           |
        |           |           |          ++-----------+-----------++           |
        |           |           |           |           |           |            |
        <------LS-1 log stream--+           |           |           |            |
        <-----------+------LS-2 log stream--+           |           |            |
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
    |  CL1  |   |  CL2  |   |  SN1  |   |  SN2  |   |  SN3  |   |  SN4  |    |  MR   |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+

### Read
TODO..

## Failure Handling

### CL Failure
CL이 append 중간에 fail될 수 있다. SN1은 전달 받은 LE를 저장하고, 같은 LS1을
저장하고 있는 다른 SN들에게 전달된다. Replication은 Primary에 저장된 LE의
순서대로 Backup들에게 저장된다. 그러므로 SN2와 SN3에 저장된 로그들의 순서가
SN1에 저장된 것과 같아야 하며, SN1에 durable하게 저장된 LE의 가장 큰 GLSN이 SN2와 
SN3에 저장된 LE의 가장 큰 GLSN보다 크거나 같다.  
만약 LS1을 저장하는 SN들의 Primary인 SN1에서 SN3로 Log Entry를 replicate하려
했으나 실패했다면, 그 이후 모든 Log Entry들은 더 이상 replicate할 수 없다. 같은
LS에 속한 모든 SN들의 Log Entry의 순서는 항상 같아야 한다.

    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
    |  CL1  |   |  CL2  |   |  SN1  |   |  SN2  |   |  SN3  |   |  SN4  |    |  MR   |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
        +--append---+----------->           |           |           |            |
      fail          |          +++          |           |           |            |
       +++          |          ||fsync      |           |           |            |
       |@|          |          +++          |           |           |            |
       |@|          |          ++--LS1------+-----------++          |            |
       |@|          |          |+--copy----->           ||          |            |
       |@|          |          |+--copy-----+----------->|          |            |
       |@|          |          ++-----------+-----------++          |            |
       |@|  X <--ack(glsn,ls1)--+           |           |           |            |
       |@|          |  |        |           |           |           |            |
       |@|          |  +-+      |           |           |           |            |
       |@|          |    |      |           |           |           |            |
       +++          |    v      |           |           |           |            |
        |           <-subscribe-+           |           |           |            |
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
    |  CL1  |   |  CL2  |   |  SN1  |   |  SN2  |   |  SN3  |   |  SN4  |    |  MR   |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+

CL이 subscribe하는 중간에 fail되면, SN으로 부터 연결되었던 스트림을 종료하면
된다.

### SN Failure
CL1이 SN1에 append 요청을 보냈으나, LS1을 복제하는 SN3에 장애가 발생해
replication 이 실패할 수 있다. 이렇게 LS1의 PB (Primary/Backup) replication이
실패한 경우, CL1은 append 싪패를 응답으로 받는다. 이런 경우, CL1은 다른 LS를
선택해서 append를 시도하면 된다.  
SN1은 replication이 실패했으므로 LS1을 finalize해야 한다. Replication 시도
중이었던 모든 append 요청은 에러를 반환한다(err_locked or err_finalized).
Finalize (or Los Stream Lock)을 시도하는 주체는 Log Stream 스스로 혹은 MR이
감지하거나 Management CL에 의해 시도될 수 있다. Finalize 관련 설명은 나중에 더
자세히 한다. Finalize된 LS는 immutable 상태고 되고, 더이상 append 요청은 받지 않는다.
CL은 finalize된 LS에게 append 요청을 하지 않는 것이 성능상 유리하다. 이미
err_finalize를 받았거나 timeout을 받은 LS에 대해서는 append를 하지 않도록
해야하며, 클러스터에 대한 정보를 갱신하기 위해 방법이 필요하다(via MR?)

    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
    |  CL1  |   |  CL2  |   |  SN1  |   |  SN2  |   |  SN3  |   |  SN4  |    |  MR   |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
        |           |           |           |           |           |            |
        |           |           |           |           |           |            |
    +-- +--append---+----------->           |         fail          |            |
    |   |           |          +++          |          +++          |            |
    |   |           |          ||fsync      |          |@|          |            |
    |   |           |          +++          |          |@|          |            |
    |   |           |          ++--LS1------+----------+@|          |            |
    |   |           |          |+--copy----->          |@|  finalize             |
    |   |           |          |+--copy-----+--> X     |@|--(locked log stream)--> ---+
    retry           |          ++-----------+----------+@|          |            |    |
    |   |           |           |          +++         |@|          |            |    |
    |   |           |           |          ||fsync     |@|          |            |    |
    |   |           |           |          +++         |@|          |            |    |
    |   <---append:error--------+           |          |@|          |            |    |
    |   |           |           |           |          |@|          |            |    |
    |   |           |           |           |          |@|         ++--LS5-------+--+ |
    +-> +--append---+-----------+-----------+----------+@+---------+>            |  | |
        |           |           |           |          |@|         |+--copy------+->| |
        |           |           |           |          |@|         |+--copy------+->| |
        |           |           |           |          |@|         ++------------+--+ |
        |           |           |           |          |@|          |            |    |
        |           |           GLSN req    GLSN req   |@|SN req    GLSN req     |    |
        |           |           *-----------*----------+@+----------*------------>    |
        |           |           |           |          |@|          |           finalization
        |           |           GLSN ack    GLSN ack   |@|SN ack    GLSN ack     |  +glsn
        |           |           <-----------<----------+@+----------<------------+ <--+
        |           |           |           |          |@|          |            |
        <--ack(glsn,ls5)--------+-----------+----------+@+----------+            |
        |           |           |           |          |@|          |            |
        |           |           |           |          |@|          |            |
        |         +-+subscribe--+-----------+----------+@+----------+--+         |
        |         | <-----------+-----------+-----+----+@|          |  |         |
        |         | |           |           |     | error on stream RPC, reconnect other
        |         | <-----------+-----------+  <--+ SN in the same LS (e.g., SN3->SN2)
        |         | |           |           |          |@|          |  |         |
        |         | <-----------+-----------+----------+@+----------+  |         |
        |         +-+-----------+-----------+----------+@+----------+--+         |
        |           |           |           |          +++          |            |
        |           |           |           |           |           |            |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+
    |  CL1  |   |  CL2  |   |  SN1  |   |  SN2  |   |  SN3  |   |  SN4  |    |  MR   |
    +-------+   +-------+   +-------+   +-------+   +-------+   +-------+    +-------+

MR은 SN들의 liveness를 감시하고 finalize (or lock)에 대한 책임을 갖고 있다.
하지만 Managment CL에서 명시적으로 특정 LS를 finalize 요청할 수 있다. 이 과정
역시 추후 설명한다.  
SN이 fail된 경우, subscribe 받고 있던 CL은 문제가 크게 영향받지 않는다. 만약
Finalize되었지만, 장애가 발생하지 않은 SN으로부터 subscribe의 스트림을 받던
경우 그대로 진행하면 된다. 만약 SN3와 같이 fail된 노드와 연결되어 있던 스트림
RPC는 같은 LS의 다른 SN에게 재접속해야 한다.

## Log Stream
Log Stream은 전체 로그의 부분 집합이고, 로그를 분산하여 저장한다. Replication
factor만큼 중복되어 저장되므로 같은 Log Stream을 여러 Storage Node가 저장하고
있다.  
LS는 각각의 Local Log Stream을 아래와 같이 저장한다. committed Log Entry는
GLSN을 부여 받은 로그이다. uncommitted Log Entry는 아직 GLSN을 부여받지 못해서
durable하지 않은 상태이다. 같은 Log Stream을 표현했다 하더라도 replication 진행
상황에 따라 서로 다른 uncommitted log entry를 갖을 수 있다. 하지만 그 숫자가
다를뿐 파일에 저장된순서는 일정하다. 왜냐하면 replication은 FIFO 방식으로
이루어지기 때문이다.   
모든 LS는 MR에게 Local Log Stream 정보를 보내야 한다. 그 정보에는 LogStreamID,
StorageNodeID와 각자 파일에 저장되었지만 아직 durable하지 않은 uncommitted log
entry의 수를 같이 보낸다. MR은 모든 Storage Node의 Log Stream에서 받은 정보를
사용해 GLSN을 할당하고 그 정보들을 Global LogStream 정보로 LS들에 알린다.
이 정보를 통해 모든 Log Stream은 commit된 Log Entry의 GLSN을 계산할 수 있다.
Local LogStream Info와 Global LogStream Info는 idempotent하도록 설계해야 한다.
Global LogStream Info는 HighestGLSN 값을 통해 보장할 수 있다. 만약 두번의 연속된
Global LogStream Info의 HighestGLSN 값에 차이가 없다면 두번째 받은 메시지에는 각
LogStreamID 마다 commit된 LE의 수가 모두 0일 것이다. Local LogStream Info도 이를
위한 방법이 필요하다 - WIP


    Log Stream
    +---+---+---+---+---+---+---+---+
    |   |   |   |   |   |   |   |   |
    |   |   |   |   |   |   |   |   |
    |   |   |   |   |   |   |   |   |
    +---+---+---+---+---+---+---+---+
      ^           ^   ^           ^
      +-----------+   +-----------+
        committed       uncommitted
                                            Local LogStream Info
    Log Stream                              +-------------------------+
    +---+---+---+---+---+---+---+---+       |- LogStreamID            |
    |   |   |   |   |   |   |   |   |   \   |- StorageNodeID          |
    |   |   |   |   |   |   |   |   |   ----+- NumUncommittedLogEntry |
    |   |   |   |   |   |   |   |   |   /   +-------+-----------------+
    +---+---+---+---+---+---+---+---+               |
      ^           ^   ^           ^                 |           +---------------+
      +-----------+   +-----------+                 +----------->               |
        committed       uncommitted                             |   Metadata    |
                                        \                       |  Repository   |
                                        ----------------------  |               |
                    .                   /                       +---------------+
                    .                     Global LogStream Info
                    .                     +------------------------------------------------+
                                          |- HighestGLSN                                   |
                                          |- vector<pair<LogStreamID,NumCommittedLogEntry>>|
    Log Stream                            +------------------------------------------------+
    +---+---+---+---+---+---+---+---+
    |   |   |   |   |   |   |   |   |
    |   |   |   |   |   |   |   |   |
    |   |   |   |   |   |   |   |   |
    +---+---+---+---+---+---+---+---+
      ^           ^   ^           ^
      +-----------+   +-----------+
        committed       uncommitted
