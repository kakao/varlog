# MR Probing of VMS

- Status: In-Progress
- Date: 2020-12-16
- Deciders: Jun.Song, Pharrell.Jang
- PR:
- Issue:

## Summary

VMS (Varlog Manager Server) 는 Varlog 클러스터에 StorageNode, LogStream, Metadata Repository 추가/삭제/변경 작업을 수행하는 중앙 서버이다. VMS 는 상태를 저장하지 않고 있기 때문에 장애에 안전하고 서버 프로세스에 이상이 있을 때 재시작하면 된다. Varlog 클러스터의 메타데이터는 MR (Metadata Repository) 가 저장하고 있기 때문에, VMS 가 구동되면 먼저 MR 에게 접속하여 클러스터 정보를 얻어온다. 하지만 지금까지의 구현은 MR 접속에 문제가 있으면 VMS 는 바로 정지되고 프로세스가 종료된다. 이러한 행동은 VMS 가 상태를 저장하지 않고 장애 발생 시 단순히 재시작하면 된다는 점에 있어서 문제가 없는 행동이지만 클러스터 관리 자동화 관점에서는 불편함을 야기한다.

이 문서는 VMS 가 MR 에게 접속 장애가 있을 때, 주기적으로 재시도를 하도록 변경하는 것을 제안한다. 더불어 VMS 가 갖고 있는 MR 접속 정보로 더이상 유효한 MR 에 접근할 수 없을 때 어떻게 해야할지를 정의한다.

## Background

현재 VMS 구현의 문제점에 대해 몇 가지 살펴본다.

- 초기 구동
  `--mr-address` 플래그 혹은 `MR_ADDRESS` 환경 변수로 전달받은 MR 에 접속이 안되는 경우, 프로세스를 즉시 종료한다.
- 접속 가능한 MR 부재
  VMS 가 접속 가능한 MR 이 없는 경우, 계속 해서 특정 MR 에게 메타데이터를 얻기 위해 시도한다. 이것은 외부에서 봤을 때 계속해서 무한 루프를 도는 것과 다를 바 없다.
  심지어

## Detailed Design

### 초기 MR 접속

MR 은 Varlog 의 모든 상태를 갖고 있는 메타데이터 저장소이다. 그러므로 상태가 없는 VMS 는 프로세스 시작과 함께 MR 에 접속하여 클러스터 상태 정보를 얻어야 한다. 하지만 MR 에 접속하지 못할 수 있는데, 클러스터 부트스트랩 과정에서 아직 MR 이 준비가 안되었거나 MR 의 장애 상황일 수 있다. 이런 상황에서 VMS 가 즉시 중단되면 클러스터 구성 과정에 불편함이 있다. 그러므로 일정 시간 혹은 일정 횟수를 재시도하도록 한다.

- `--init-mr-conn-retry-count`:
  - 초기 MR 접속 재시도 횟수 (기본값: -1)
  - -1 이면 계속해서 접속 시도를 한다.
- `--init-mr-conn-retry-backoff`
  - 초기 MR 접속 재시도를 할 때, 어느 정도의 기간을 쉬었다가 재시도를 할지 결정한다. (기본값: 100ms)

### 접속 가능한 MR 부재

VMS 는 SN 들을 주기적으로 모니터링하는 SNWatcher 를 갖고 있다. SNWatcher 는 먼저 클러스터의 메타데이터를 얻고, 그 정보를 바탕으로 SN 이 살아있는지 검사를 한다. 하지만 MR 로부터 클러스터 정보를 얻을 수 없다면 SNWatcher 의 모니터링 작업은 계속해서 실패하므로 VMS 는 정상 동작을 하지 않고 프로세스는 종료하지 않는다. 이러한 현상은 외부에서 VMS 가 정상 동작을 하고 있는지 아닌지 확인하기 어렵게 만든다. 이 문제를 해결하기 위해 아래 장치를 두어야 한다.

- VMS Healthcheck
  - VMS gRPC 서비스에 Healthcheck 를 제공하여, VMS 가 정상 동작하는지 체크한다.
  - SNWatcher 뿐만 아니라 다른 기능이 정상 동작할 수 없는 상태라면, Healthcheck 는 이를 응답에 알리고, Devops 혹은 클러스터 자동화 도구가 VMS 에 대한 조치를 할 수 있도록 도와준다.

## Alternatives

## Open Issues



