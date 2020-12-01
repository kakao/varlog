# API Design of Subscribe

- Status: accepted
- Date: 2020-11-26
- Deciders: Jun.Song, Pharrell.Jang
- PR: [VARLOG-299](https://jira.daumkakao.com/browse/VARLOG-299), [#258](https://github.daumkakao.com/varlog/varlog/pull/258)
- Issue: [VARLOG-281](https://jira.daumkakao.com/browse/VARLOG-281), [#259](https://github.daumkakao.com/varlog/varlog/pull/259)

## Summary

`Subscribe`는 클라이언트가 로그를 읽는 가장 주된 수단이다. `Subscribe`의 API signature와 구현 방식에 대해 설명한다. 그리고 `Subscribe` 가 읽을 범위를 지정할 수 있게 도움을 주는 새로운 API (`Head`, `Tail`) 에 대해서도 설명한다.

## Background

`Subscribe`는 클러스터의 모든 Log Stream으로부터 데이터를 스트리밍 받고, 각 결과로부터 순서를 보장하여 사용자에게 데이터를 제공해야 한다. 즉, 모든 Log Stream Replica에게 RPC를 요청하고 응답을 관리해야 하며, 사용자에게는 데이터의 순서 보장 및 문제 발생 시 에러를 반환하도록 해야한다.

## Detailed Design

클라이언트가 사용하는 Subscribe API는 다음과 같은 signature 를 갖는다.

```go
type OnNext func(glsn types.GLSN, err error)
func Subscribe(ctx context.Context, begin types.GLSN, end types.GLSN, f OnNext, opts SubscribeOption) error
func Head(ctx context.Context) (types.GLSN, error)
func Tail(ctx context.Context) (types.GLSN, error)
```

`Subscribe`는 [begin, end) 범위의 로그를 스트리밍 받고, 로그 위치 순서에 맞춰 콜백 함수 `f` 를 호출한다. 이때 클러스터가 저장하고 있는 로그의 가장 처음부터 스트리밍 받고 싶다면, `Head` 를 통해서 적절한 begin을 알아낼 수 있다. 마찬가지로, 가장 최근에 저장된 로그부터 스트리밍하려면, `Tail ` 을 통해서 적절한 시작 위치를 알아낼 수 있다.

`Subscribe` 를 구현하기 위해 아래와 같은 설계를 갖는다.


     +----------------------+                                                    
     |Subscribe API Handler |                                                    
     |     <goroutine>      |                                                    
     +----------------------+                                                    
                 |                                                               
                 |   +----------------------+                                    
                 |   |      subscriber      |                                    
                 |   |     <goroutine>      |                                    
                 +-->|     +----------+     |                                    
                 |   |     |  queue   |--------+         +----------------------+
                 |   |     +----------+     |  |         |     transmitter      |
                 |   +----------------------+  +-------->|     <goroutine>      |
                 |                             |         +----------------------+
                 |   +----------------------+  |                     |           
                 |   |      subscriber      |  |                     v           
                 |   |     <goroutine>      |  |         +----------------------+
                 +-->|     +----------+     |  |         |SubscribedEnriesQueue |
                 |   |     |  queue   |--------+         +----------------------+
                 |   |     +----------+     |  |                     |           
                 |   +----------------------+  |                     v           
                 |                             |         +----------------------+
                 |                             |         |      dispatcher      |
                 |             ...             |         |     <goroutine>      |
                 |                             |         +----------------------+
                 |                             |                     |           
                 |   +----------------------+  |                     v           
                 |   |      subscriber      |  |         +----------------------+
                 |   |     <goroutine>      |  |         |        OnNext        |
                 +-->|     +----------+     |  |         +----------------------+
                     |     |  queue   |--------+                                 
                     |     +----------+     |                                    
                     +----------------------+                                                                                                     
`Subscribe` 가 호출되면, 각 Log Stream에게 데이터를 구독받는 `subscriber` 고루틴을 생성한다. `subscriber` 고루틴은 데이터를 스트리밍 받고, 연결에 문제가 발생 시 에러를 처리한다. `subscriber`는 수신한 데이터를 내부 큐에 저장한다. `transmitter`는 `subscriber`의 내부 큐에 새로운 데이터가 저장될 때마다 `SubscribedEntriesQueue`에 저장할 수 있는지, 즉 로그의 순서가 맞는지 체크한다. `dispatcher` 고루틴은 `SubscribedEntriesQueue`에서 데이터를 읽고 `OnNext` 함수를 호출한다.

### Challenges

`Subscribe` API 구현에 있어 몇 가지 해결해야할 문제들이 있다.

- begin 에 해당하는 로그가 없는 경우
- hole 이 있는 경우
- Log Stream Replica가 Fail된 경우

`Subscribe`로 요청한 로그 범위가 `Trim`된 범위와 겹친다면, `Subscribe` 요청의 리턴값으로 에러를 받을 것이다. 하지만 `Subscribe` 요청과 `Trim` 요청이 동시에 일어난 경우, `Subscribe` 는 정상적으로 리턴되지만 `subscriber` 가 받는 로그 데이터에는 begin 위치에 있는 로그가 없을 수 있다. 이런 경우, 모든 `subscriber` 에는 `SubscribedLogEntriesQueue`에 저장할 데이터가 없다. 이런 경우, 에러가 발생하며, `OnNext` 함수를 통해 에러를 처리할 수 있다.

로그에 hole이 있는 경우는 `Trim` 에 의한 hole과 특정 Log Stream에 장애가 발생한 경우이다. 이 경우에도, `transmitter` 고루틴에 의해 hole이 감지될 수 있고, 위와 같이 에러를 처리할 수 있다.

Log Stream Replica에 문제가 발생하여 연결이 끊기거나 잘못된 응답이 온 경우, `subscriber`는 이를 알아내고 에러 처리를 해야 한다.

에러 처리는 모두 `OnNext` 에 `types.InvalidLogEntry` 와 적절한 에러 값을 넘겨주면 된다.

## Alternatives

-

## Open Issues

다음과 같은 문제를 더 고민해 볼 수 있다.

- Prefetch
  - `Subscribe` 를 사용해 로그를 계속해서 수신하려면, prefetch를 적절히 사용해야 한다.
- HoleHandlePolicy
  - 어떤 애플리케이션의 경우, Hole에 대해 덜 민감하게 동작할 수 있다. 이런 요구 사항을 위해 HoleHandlePolicy를 정의할 수 있다.
- 새롭게 참여한 스토리지 노드
  - 새롭게 참여한 스토리지 노드를 클라이언트가 발견하지 못할 수 있다. 이런 경우, Hole이 발생하게 되는데, 에러 처리할 때 새롭게 SN 목록을 갱신하고 다시 Subscribe 요청을 할 수 있다.
  - 위의 방법은 비효율적이므로 더 좋은 방법을 고안해야한다.
