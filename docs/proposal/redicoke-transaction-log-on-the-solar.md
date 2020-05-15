# Redicoke: transaction log on the solar
* Author(s): Jun.Song
* Approver:
* Status: Draft
* Implemented in: 
* Last updated: 
* Discussion at: 

## Abstract

## Background
Redicoke는 Key/Value 저장소인 Coke에 Redis 프로토콜을 추가하고 Redis의
자료구조를 구현했다. Redis의 List, Map, Set에 하나의 엔트리에 대한 읽기/쓰기는
Coke의 여러 키에 대한 I/O를 발생시킨다. Redicoke는 여러 키에 대한 접근을
atomic하게 처리 하기 위해 트랜잭션을 지원하고 있다. Redicoke의 트랜잭션 처리는
Coke에 쓰기 작업 진행 중인 Key에 대한 백업 이미지를 저장하고 CAS (Compare & Set)
연산을 통해 이루어진다.
Coke는 키가 많고 랜덤하게 접근되는 액세스 패턴을 가정한 Key/Value 저장소이다.
트랜잭션에 대한 처리 (e.g., Transaction Log 활용)에 더 적절한 액세스 패턴을 갖는
저장소를 사용해 Redicoke의 성능을 향상시킬 수 있다. 이 문서는 SOLAR를 사용해
Redicoke의 트랜잭션을 어떻게 구현할 수 있는지, 어떤 장점을 갖는지를 설명한다.

## Proposal

## Rationale

## Implementation

## Open issues

