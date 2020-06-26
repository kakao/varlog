## 차례
<!-- vim-markdown-toc GFM -->

* [개요](#)
* [주요 구성 요소](#--)
    * [Storage Node](#storage-node)
    * [Metadata Repository](#metadata-repository)
    * [Sequencer](#sequencer)
    * [Client](#client)
* [설계와 구현](#-)
    * [Read](#read)
        * [Projection](#projection)
    * [Append](#append)
    * [Fill](#fill)
    * [Seal](#seal)
    * [Trim](#trim)
    * [Projection 관리](#projection-)
    * [Replication Policy](#replication-policy)
    * [클러스터 생성](#--1)
    * [Append / Read Failure](#append--read-failure)
    * [Storage Node Lease Timeout](#storage-node-lease-timeout)

<!-- vim-markdown-toc -->

## 개요
WOKL(World of Kakao Logs)은 total order를 엄격히 지키는 분산 로그 저장소이다.
WOKL은 strong consistency를 제공하며, SMR(state machine replication),
transaction log , consensus 등에 활용할 수 있다. 이를 확장하면 fault-tolerance,
recovery system, distributed lock, leader election, consistent medata storage,
transactional database 등을 구현할 수 있다.
WOKL은 로그를 읽고 쓰는 간단한 인터페이스를 제공한다. 여러 스토리지 노드에
저장되는 분산 로그를 추상화한 간단한 인터페이스들을 통해 카카오의 분산
시스템들에 strong consistency를 제공한다. 카카오의 여러 분산 스토리지들은 높은
대역폭과 저지연 I/O를 제공하는 대용량 및 고성능 시스템이다. 이들 시스템에게
분산 로그의 strong ordering을 제공하고, 분산된 시스템 인스턴스들 간의
consistency와 consensus를 제공하기 위해 WOKL 역시 고성능 시스템이어야 한다.
기존 카카오 스토리지들은 각각의 시스템 요구 사항에 맞는 consistency를 제공하고
있다. Redicoke, Coke, Kage 등은 매우 규모가 큰 서비스들의 요구 사항에 맞추어
설계 및 구현되었으며, 만족할만한 성능과 상황에 맞는 consistency or eventual
consistency를 제공한다. 하지만 이 시스템들의 data consistency module은
monolithic하게 다른 코드 베이스에 강하게 결합되어 있어 새로운 요구 사항을
만족시키거나 추가적인 성능 개선 및 디버깅을 어렵게 한다. WOKL은 strong
consistency를 위해 단순한 로그 읽기/쓰기 인터페이스를 제공하고 기존 코드
베이스에서 잘 분리된 시스템을 제공한다. 따라서 WOKL은 구조적 간단함으로 기능
확장에 대한 용이성을 제공하면서 고성능 분산 로그 시스템을 제공한다. 예를 들면,
WOKL을 사용하여 다양한 isolation level을 제공하는 transaction manager를 구현할
수 있으며, 이는 Redicoke와 같은 atomic operation을 제공하는 Key/Value Storage에
활용할 수 있다.

## 주요 구성 요소
WOKL의 주요 구성 요소는 Metadata Repository, Storage Node, Sequencer, Client
이다.

### Storage Node
스토리지 노드는 로그를 저장하는 디스크 서버이다. 로그는 여러 대의 Storage Node에
나누어 저장된다. Storage Node는 로그를 저장하기 위해 Log Stream이라는 추상화를
사용한다. Storage Node는 여러 개의 Log Stream을 갖을 수 있다. Log Stream은 여러
Log Segment의 나열로 이루어진다. Log Segment는 로그를 저장하는 파일에 대한
추상화이다. Log Segment는 Log Entry의 나열로 이루어져 있으며, Log Entry는 순서를
나타내는 고유한 값인 GLSN (Global Log Sequence Number)를 갖는다.

    ────────────────────────────Log Stream───────────────────────────────▶

    ┌──────────────────┐  ┌──────────────────┐  ┌──┬──┬──┬──┬──────┐
    │                  │  │                  │  │  │  │  │  │      │
    │   Log Segment    │  │   Log Segment    │  │  │  │t1│t2│ ...  │   ...
    │                  │  │                  │  │  │  │  │  │      │
    └──────────────────┘  └──────────────────┘  └──┴──┴──┴──┴─▲────┘
                                                              │
                                                          next LSN
                                                              │

t1, t2는 Log Stream에 확실하게 쓰여진 Log Entry이다. WOKL은 이 로그를 읽는 모든
클라이언트에게 t1, t2 순서의 consistent view를 제공한다. Storage Node는 LSN과
디스크 위치에 대한 매핑 테이블을 메모리에 관리한다. 특정 LSN에 대한 읽기 요청은
메모리 접근을 통해 빠르게 디스크 위치를 알아내고, Read I/O가 이루어진다.
(최적화를 위한 로그 데이터 Cache는 여기서 다루지 않는다.)

로그의 전체 주소 공간은 무한하며 로그는 각 Log Stream으로 stripe되어 저장된다.
Storage Node는 여러 개의 SSD로 이루어지고, 각 SSD에 대해 하나의 Log Stream이
존재하는 것을 최적의 deployment라 가정한다. Log Stream의 I/O는 SSD의 bandwidth를
saturation하도록 구현되어야 한다.

    ┌──────────────────┐┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
    │                  ││   │   │   │   │   │   │   │   │   │   │
    │GLSN address space││ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │10 │
    │                  ││   │   │   │   │   │   │   │   │   │   │
    └──────────────────┘└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
              │
              │    ┌──────────────────┐┌───┬───┬───┬───┬───┐
              │    │                  ││   │   │   │   │   │
              ├───▶│   Log Stream 1   ││ 1 │ 3 │ 5 │ 7 │ 9 │
              │    │                  ││   │   │   │   │   │
              │    └──────────────────┘└───┴───┴───┴───┴───┘
              │
              │    ┌──────────────────┐┌───┬───┬───┬───┬───┐
              │    │                  ││   │   │   │   │   │
              └───▶│   Log Stream 2   ││ 2 │ 4 │ 6 │ 8 │10 │
                   │                  ││   │   │   │   │   │
                   └──────────────────┘└───┴───┴───┴───┴───┘

### Metadata Repository
전체 클러스터의 Storage Node 구성에 대한 정보를 저장하기 위해 Metadata
Repository를 사용한다. Storage Node의 Log Stream이 포함하는 GLSN 범위에 대한
정보를 Projection이라고 한다. Projection을 통해 로그를 읽고 쓰기 위한
논리적인 로그 위치 (GLSN)로 부터 Storage Node와 Log Stream 정보를 알 수 있다.
또한 Replication을 위한 설정 정보도 포함되어 있다. Projection은 버전 정보를 붙여
관리하며, 이 버전을 epoch라고 부른다.
Metadata Repository는 Projection을 포함한 클러스터의 주요 설정 정보(e.g.,
Sequencer에 대한 정보 등)를 저장하는 ground truth storeage이다. 그러므로
consistent view를 제공해야하며, 이는 paxos나 raft 등을 사용하여 제공한다.
WOKL은 이를 위해 Apache Zookeeper 혹은 etcd를 활용한다.

### Sequencer
Sequencer는 Client가 Log Entry를 쓰기 위한 GLSN을 알려준다. Client는
Sequencer에게 1개 이상의 로그 위치를 요청할 수 있다. Sequencer는 Client에게
응답한 LSN 증가량만큼 내부의 counter 값을 증가시킨다. Sequencer를 사용해 Log
Entry를 저장할 GLSN을 발급하는 과정은 Fast Log Tail Finding 이다. Sequencer를
사용하지 않고 LSN을 찾는 Slow Log Tail Finding 방식도 있다. Slow Log Tail
Finding은 Sequencer Fail이 발생한 경우 필요하다. 이 부분은 나중에 자세히
설명한다.

### Client
Client는 라이브러리 형식으로 제공된다. WOKL의 주요 동작은 Client Library를 통해
시작되며, 다른 구성 요소들은 상대적으로 passive 한 동작을 한다. deployment
시에는 이 라이브러리를 사용하여 로그 읽기/쓰기를 수행하는 proxy 서버를 둘 것으로
예상된다. 애플리케이션들은 Client 역할을 하는 서버에게 Read/Write 요청을
보내거나 Client Library를 직접 사용하여 로그를 읽거나 쓸수 있다.

## 설계와 구현
### Read
Client는 특정 로그 위치를 읽기 위해 Read 명령어를 사용한다. 로그 위치(GLSN)와
epoch를 인자로 사용하며, Local에 저장된 Projection을 사용해 로그 위치에 해당하는
Storage Node를 알아낸다.
Storage Node는 Client가 보낸 epoch와 자신이 알고 있는 것을 비교한다. 만약 epoch
값이 서로 다르다면 에러를 반환한다. 요청 받은 로그 위치가 아직 쓰여지지 않았다면
에러를 반환한다.
Replication policy가 read에 영향을 어떻게 주는지는 나중에 설명한다.

      ┌──────────────┐     ┌──────────────┐
      │    Client    │     │ Storage Node │
      └──────────────┘     └──────────────┘
              │                    │
            ┌─┤                    │
            │ │                    │
    get storage node               │
       information                 │
            │ │                    │
            └─▶                    │
              ├───────read─────────▶
              │                    ├─┐
              │                    │ │
              │             verify epoch & log
              │                  position
              │                    │ │
              │                    ◀─┘
              ◀───────reply────────┤
              │                    │
              │                    │
      ┌──────────────┐     ┌──────────────┐
      │    Client    │     │ Storage Node │
      └──────────────┘     └──────────────┘

#### Projection
*TODO: LGSN으로 Storage Node와 Physical Position을 알 수 있는 방법 필요?*


Projection은 Metadata Repository에 저장되는 버전(epoch)이 붙은 클러스터에 대한
설정 정보의 일부이다. Client는 Projection을 사용해 로그의 논리적인 위치를
저장하는 Storage Node 정보를 알아낸다. 그림 2에서 GLSN이 각 Log Stream에 round
robin 방식으로 할당되었다. Projection은 Storage Node의 구성, 각 Storage Node에
속한 Log Stream의 구성, 각 Log Stream이 맡고 있는 GLSN의 범위 등이 포함된다.

현재까지는 설계의 단순함을 위해 Replica Group은 Storage Node 단위로 정해진다
(그림 a). 더 복잡한 구조에서는 Log Stream 단위로 Replica Group을 만들 수 있다
(그림 b).

그림 a는 설계와 구현이 단순하지만, 운영상 제약이 따른다. Replica Group의 Storage
Node들은 같은 수의 Log Stream을 갖는다. 성능을 고려하여 하나의 SSD에 하나의 Log
Stream을 생성한다면, 같은 Replica Group 내의 Storage Node들은 모두 같은 수의
SSD를 갖고 있어야 한다.

       ┌ ─Replica Group─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
                                                          │
       │ ┌──────────────┐┌──────────────┐┌──────────────┐
         │ Storage Node ││ Storage Node ││ Storage Node │ │
       │ │              ││              ││              │
         │ ┌──────────┐ ││ ┌──────────┐ ││ ┌──────────┐ │ │
       │ │ │Log Stream├─┼┼─▶Log Stream├─┼┼─▶Log Stream│ │
         │ ├──────────┤ ││ ├──────────┤ ││ ├──────────┤ │ │
       │ │ │Log Stream◀─┼┼─┤Log Stream◀─┼┼─┤Log Stream│ │
         │ ├──────────┤ ││ ├──────────┤ ││ ├──────────┤ │ │
       │◀┼─┤Log Stream◀─┼┼─┤Log Stream│ ││ │Log Stream◀─┼─
         │ └──────────┘ ││ └──────────┘ ││ └──────────┘ │ │
       │ └──────────────┘└──────────────┘└──────────────┘
        ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘

       (a) storage nodes forming replica group




       ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
       │   Storage Node   │ │   Storage Node   │ │   Storage Node   │ │   Storage Node   │
       │                  │ │                  │ │                  │ │                  │
    replica group─────────────────────────────────────────────────┐ │ │                  │
    │  │   ┌──────────┐   │ │   ┌──────────┐   │ │   ┌──────────┐ │ │ │   ┌──────────┐   │
    │  │   │Log Stream│   │ │   │Log Stream│   │ │   │Log Stream│ │ │ │   │Log Stream│   │
    │  │   └──────────┘   │ │   └──────────┘   │ │   └──────────┘ │ │ │   └──────────┘   │
    └─────────────────────────────────────────────────────────────┘ │ │                  │
       │                  │ │ ┌─────────────────────────────────────────────────replica group
       │   ┌──────────┐   │ │ │ ┌──────────┐   │ │   ┌──────────┐   │ │   ┌──────────┐   │  │
       │   │Log Stream│   │ │ │ │Log Stream│   │ │   │Log Stream│   │ │   │Log Stream│   │  │
       │   └──────────┘   │ │ │ └──────────┘   │ │   └──────────┘   │ │   └──────────┘   │  │
       │                  │ │ └─────────────────────────────────────────────────────────────┘
       │                  │ │                  │ │                  │ │ replica group────┼──
       │   ┌──────────┐   │ │   ┌──────────┐   │ │   ┌──────────┐   │ │ │ ┌──────────┐   │
       │   │Log Stream│   │ │   │Log Stream│   │ │   │Log Stream│   │ │ │ │Log Stream│   │
       │   └──────────┘   │ │   └──────────┘   │ │   └──────────┘   │ │ │ └──────────┘   │
       │                  │ │                  │ │                  │ │ └────────────────┼──
       │                  │ │                  │ │                  │ │                  │
       └──────────────────┘ └──────────────────┘ └──────────────────┘ └──────────────────┘

       (b) log streams forming replica group


### Append
Client는 Sequencer에게 로그를 append할 위치(GLSN)을 알아낸다. Projection을 통해
알고 있는 Storage Node들에게 write operation을 전달한다.
서버는 Client가 보낸 epoch가 자신이 알고 있는 것과 다르다면 에러를 반환한다.
만약 로그 위치가 이미 쓰여진 경우에도 에러를 반환한다.
Replication policy에 따른 append 과정은 추후 설명한다.

      ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
      │    Client    │     │ Storage Node │     │  Sequencer   │
      └──────────────┘     └──────────────┘     └──────────────┘
              │                    │                    │
              │                    │                    │
              ├──────────get next log position──────────▶
              │                    │                    │
              ◀────────────next log position────────────┤
            ┌─┤                    │                    │
            │ │                    │                    │
    get storage node               │                    │
       information                 │                    │
            │ │                    │                    │
            └─▶                    │                    │
              ├──────append────────▶                    │
              │                    ├─┐                  │
              │                    │ │                  │
              │             verify epoch & log          │
              │                  position               │
              │                    │ │                  │
              │                    ◀─┘                  │
              ◀───────reply────────┤                    │
              │                    │                    │
              │                    │                    │
      ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
      │    Client    │     │ Storage Node │     │  Sequencer   │
      └──────────────┘     └──────────────┘     └──────────────┘

### Fill
Client는 log 중 특정 위치가 hole이라 판단하고 그곳에 junk를 채울수 있다. 로그에
hole이 생기는 주된 원인은 log position을 받은 client가 write 하지 못하고
fail되는 경우이다.
로그의 hole을 채우기 위해 Client는 해당 로그 위치에 대한 fill 명령을 Storage
Node에게 전송한다. 해당 로그 위치에 Junk 값을 쓰는 것이므로 Storage Node는
append 명령어를 받은 것과 같이 반응한다.

### Seal
Storage Node 구성 변경 등의 이유로 Projection이 변경이 되면, Client는 seal
명령을 Storage Node에게 전달한다. Client는 자신의 epoch를 전달하고 Storage
Node는 epoch가 동일한지 검사한다. Storage Node는 현재 진행 중인 명령들을 모두
취소한다. 그리고 자신의 epoch를 증가시키고 자신이 저장하고 있는 로그의 가장 최근
위치와 epoch를 반환한다.

Client가 seal 명령어를 사용하여 Reconfiguration 과정을 시작할때, 모든 Storage
Node에게 seal 명령어를 보낸다. seal에 대한 응답은 각 replica group에 있는
Storage Node들중 하나에게서만이라도 받으면 된다. 예를 들어, 특정 replica group을
구성하는 log stream들을 호스팅하는 Storage Node들중 하나만 seal이 된다면 그
주소에 대한 append 명령은 실패할 것이다.

### Trim
Trim은 요청 받은 위치 이하의 로그를 삭제하는 요청이다.

### Projection 관리
*TODO: Metadata Repository와 Client 사이에 Projection에 대한 관리*

### Replication Policy

### 클러스터 생성
클러스터는 관리자 역할을 하는 클라이언트에 의해 생성된다. Storage Node는 Log
Stream을 저장할 공간을 준비(format)한다. Sequencer는 로그 위치를 0으로
초기화한다. Metadata repository 역시 클러스터 메타데이터를 저장하기 위한 준비를
한다. 클러스터 생성에 참여하는 모든 인스턴스는 각자의 로컬 epoch를 0으로
초기화한다.
클라이언트는 먼저 metadata repository로부터 현재의 클러스터 정보(Projection)을
얻는다. 그 이후 Reconfiguration 과정을 거친다. 이를 위해 우선 각 Storage Node에게
seal 명령어를 전달한다. 클러스터 관리 역할을 하는 클라이언트 이므로 새로운
Storage Node에 대한 정보를 알고 있다고 가정한다. *(TODO: Node Discovery 필요?)*
모든 Storage Node에 대한 seal 명령어가 성공하면 새로운 Projection을 구성해서
Metadata Repository에게 제안한다. Metadata Repository는 PAXOS나 RAFT 등의
Consensus Algorithm으로 구성된 저장소이다. Client의 새로운 Projection이
받아들여지면, 로그를 쓰고 읽을 수 있다.

    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
    │    Client    │    │ Storage Node │    │  Sequencer   │    │  Meta Repos  │
    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
            │                   │                   │                   │
            ├─┐                 ├─┐                 ├─┐                 ├─┐
            │ │                 │ │                 │ │                 │ │
            │ init              │format             │ init              │ init
           (epoch = 0)         (epoch = 0)         (glsn = 0)          (epoch = 0)
            │ │                 │ │                 │ │                 │ │
            ◀─┘                 ◀─┘                 ◀─┘                 ◀─┘
            │                   │                   │                   │
            │                   │                   │                   │
            ├──get current projection───────────────┼───────────────────▶
            │                   │                   │                   │
            ├──seal (epoch = 1)─▶                   │                   │
            │                   │                   │                   │
            ├──propose new configuration ───────────┼───────────────────▶
            │  (storage node & sequencer)           │                   │
            │                   │                   │                   │
            │                   │                   │                   │
    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
    │    Client    │    │ Storage Node │    │  Sequencer   │    │  Meta Repos  │
    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘

여기서 주의할 점은 클러스터가 생성되는 과정에서 Storage Node는 자신이 호스팅하는
Log Stream에 어떤 로그 주소 범위가 쓰여지는지를 모른다. Append하는 Client는
Projection을 통해 Sequencer로부터 발급받은 GLSN이 어떤 Storage Node의 Log
Stream에 저장되는지 모두 알고 있다. 즉, Projection의 버전 정보인 epoch가 항상 잘
관리되어야 한다.

### Append / Read Failure
Client 1은 append를 시도한다. Storage Node 1에 장애가 발생하여 append 명령에
대한 응답을 받지 못하게 된다면, Reconfiguration 과정을 거쳐야 한다. 모든 Storage
Node에 seal 명령어를 보낸다. 이때 새로운 epoch를 제안하고 그 epoch가 Storage
Node의 epoch보다 크면 Storage Node의 epoch는 갱신된다. 그림에서 Storage Node 2는
epoch가 2로 갱신되었다. Client 1은 seal 명령어의 응답으로 받은 Storage Node의
정보를 사용해 새로운 Projection을 만들고 Metadata Repository에 제안한다. 이것이
받아들여지면 Client 1은 새로운 projection과 epoch를 사용하여 다시 append 를
시도한다. Client 1이 다시 append를 시도할 때, Sequencer 로부터 새로운 LGSN을
받을지는 아직 결정되지 않았다. (LGSN의 lease time이 있을 수도 있다?)
Client 2는 read 명령을 시도한다. 하지만 새롭게 Projection이 변경된 것을 알지
못한다. read 명령 또한 Storage Node 1의 장애로 실패하고 Reconfiguration을
시도한다. 이를 위해 우선 seal 명령어를 시도하고 응답을 통해 Client 2는 자신의
epoch가 오래되었음을 알게 된다. 이런 경우, Client 2는 Metadata Repository로부터
현재의 Projection을 받고 자신의 상태를 갱신한다. 그 이후 다시 read 명령어를
시도한다.

    ┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐
    │   Client 1   ││   Client 2   ││Storage Node 1││Storage Node 2││  Sequencer   ││  Meta Repos  │
    └──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘
            │               │               │               │               │               │
       epoch = 1       epoch = 1       epoch = 1       epoch = 1        glsn = 0       epoch = 1
            │               │               │               │               │               │
            ├────next───────┼───────────────┼───────────────┼───────────────▶               │
            │               │               │               │               │               │
            ├────append─────┼─────────────▶ ╳               │               │               │
            │               │               │               │               │               │
            ├────seal───────┼─────────────▶ ╳               │               │               │
            │               │               │               │               │               │
            ├────seal───────┼───────────────┼───────────────▶               │               │
            │               │               │               │               │               │
            │               │               │          epoch = 2            │               │
            │               │               │               │               │               │
            ├────propose new│configuration──┼───────────────┼───────────────┼───────────────▶
            │               │               │               │               │               │
       epoch = 2            │               │               │           glsn = 1       epoch = 2
            │               │               │               │               │               │
            ├────next───────┼───────────────┼───────────────┼───────────────▶               │
            │               │               │               │               │               │
            ├────append─────┼───────────────┼───────────────▶               │               │
            │               │               │               │               │               │
            │               │               │               │               │               │
            │               ├────read─────▶ ╳               │               │               │
            │               │               │               │               │               │
            │               ├────seal─────▶ ╳               │               │               │
            │               │               │               │               │               │
            │               ├────seal───────┼───────────────▶               │               │
            │               │               │               │               │               │
            │               ├────renew configuration────────┼───────────────┼───────────────▶
            │               │               │               │               │               │
            │          epoch = 2            │               │               │               │
            │               │               │               │               │               │
            │               ├────read───────┼───────────────▶               │               │
            │               │               │               │               │               │
            │               │               │               │               │               │
    ┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐
    │   Client 1   ││   Client 2   ││ Storage Node ││ Storage Node ││  Sequencer   ││  Meta Repos  │
    └──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘

위의 시나리오는 Storage Node가 장애가 발생하는 상황이다. 만약 Storage Node 의
네트워크가 파티션 되거나 혹은 Client와 Storage Node가 파티션 되어 서로 정상으로
착각하는 상황이 발생하면 좀더 복잡해진다. 이 문제들은 나중에 더 설명한다.

Client 1이 append 하는 중간에 어떤 문제로 append를 제대로 완료하지 못한 두가지
상황을 아래 그림으로 설명한다. Storage Node 1과 Storage Node 2에는 같은 Log
Stream에 대한 replica group을 형성하고 있으며, Chain Replication 순서는 Storage
Node 1에 먼저 저장하고 성공하면 Storage Node 2에 저장한다. 2개의 replica를
유지하며, 1개의 Storage Node에 문제가 발생해도 Availability를 제공한다고
가정한다. (f = 1)
첫번째는 Client 1이 Storage Node 1에 append는 성공했으나 Storage Node 2에는
실패했다. 해당 로그 위치를 읽으려는 Client 2는 replication chain 상 가장
마지막인 Storage Node 2에 read를 시도한다. 하지만 Storage Node 2에는 아직 해당
위치에 Log Entry가 쓰여져 있지 않으므로 No Entry 에러를 반환한다. Client 2가
해당 로그 위치를 복구할수 있다. 먼저 Replication Chain 상 가장 첫 노드인 Storage
Node 1에 해당 위치에 대한 read를 시도한다. 이 read가 성공하면, 그 값을 사용해
다른 Storage Node들에 append를 시도한다.
두번째 경우에도 Client 1이 append 명령어 시도 중 문제가 생긴다. 하지만
Replication Chain 상 첫번째인 Storage Node 1에 대한 append 시도부터 실패한다.
Client 2는 해당 위치에 대해 복구를 시도하지만, Storage Node 1에서부터 Log
Entry가 없기 때문에 fill 명령어를 사용해 Storage Node 들에 junk 값을 채운다.

    ┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐
    │   Client 1   ││   Client 2   ││Storage Node 1││Storage Node 2││  Sequencer   ││  Meta Repos  │
    └──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘
            │               │               │               │               │               │
       epoch = 1       epoch = 1       epoch = 1       epoch = 1        lgsn = 0       epoch = 1
            │               │               │               │               │               │
            ├────next───────┼───────────────┼───────────────┼───────────────▶               │
          ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐     │               │               │
            ├────append─────┼───────────────▶   chain replication           │               │
          │ │               │               │         │     │               │               │
            ├────append─────┼───────────────┼─────▶ ╳       │               │               │
          └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘     │               │               │
            │               │               │               │               │               │
            ╳               │               │               │               │               │
            │               ├────read / ENOENT──────────────▶               │               │
            │               │               │               │               │               │
            │             ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐             │               │
            │               ├────read───────▶              recovery         │               │
            │             │ │               │               │ │             │               │
            │               ├────write──────┼───────────────▶               │               │
            │             └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘             │               │
            │               │               │               │               │               │
         ╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳
            │               │               │               │               │               │
            │               │               │               │               │               │
            ├────next───────┼───────────────┼───────────────┼───────────────▶               │
            │               │               │               │               │               │
            ├────append─────┼─────▶  ╳      │               │               │               │
            │               │               │               │               │               │
            ╳               │               │               │               │               │
            │               ├────read / ENOENT──────────────▶               │               │
            │               │               │               │               │               │
            │             ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐             │               │
            │               ├────read / ────▶            junk filling       │               │
            │             │ │    ENOENT     │               │ │             │               │
            │               │               │               │               │               │
            │             │ ├────fill───────▶               │ │             │               │
            │               │               │               │               │               │
            │             │ ├────fill───────┼───────────────▶ │             │               │
            │              ─│─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─│─              │               │
            │               │               │               │               │               │
            │               │               │               │               │               │
    ┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐
    │   Client 1   ││   Client 2   ││ Storage Node ││ Storage Node ││  Sequencer   ││  Meta Repos  │
    └──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘

### Storage Node Lease Timeout
Storage Node가 네트워크 파티션에 의해 자신은 정상 동작인 것으로 착각하지만
실제로는 장애가 생긴 상황일 수 있다. 특히 일부 Client가 파티션된 네트워크 그룹에
같이 속해 있다면, 이 Client와 Storage Node는 정상 동작을 하려고 한다. Client는
Sequencer의 LGSN을 계속해서 증가시키거나 성공하지 못하는 append를 계속 시도 혹은
read 실패를 할 수 있다. 이런 상황에 대해 살펴보고 해결방식에 대해 설명한다.


