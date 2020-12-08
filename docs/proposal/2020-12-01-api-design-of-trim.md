# API Design of Trim

- Status: in-progress
- Date: 2020-12-01
- Deciders: Jun.Song, Pharrell.Jang
- PR
- Issue:

## Summary

`Trim` 은 Varlog에 저장된 로그를 삭제하고 디스크 공간을 비운다. 이 문서는 `Trim` 의 Signature와 설계를 기술한다.

## Background

Varlog에 저장된 로그는 append만 가능하며 immutable이다. 디스크 공간을 비우기 위해 로그를 삭제해야 하는 `Trim` API를 제공하며, 이 API는 주어진 로그 위치보다 작은 로그들을 모두 삭제한다. Varlog 는 현재 읽기 중인 로그 범위가 `Trim` 에 의해 보호되는 것을 보장하지 않는다. 즉, `Subscribe` 중인 로그 범위가 `Trim` 에 의해 삭제될 수 있다. 그러므로 devops 엔지니어는 `Trim` 이 안전하게 진행될 수 있도록 주의를 기울여야 한다.

## Detailed Design

클라이언트는 아래 `Trim` API를 사용하여 로그를 삭제할 수 있다. `Trim`은 `until` 로 지정된 로그 위치까지의 모든 로그를 삭제한다.

```go
func Trim(ctx context.Context, until types.GLSN) error
```

모든 `Trim` 의 데이터 삭제는 비동기적으로 실행된다. 즉, 클라이언트가 `Trim` 요청을 하면 즉시 응답을 받지만, 스토리지 노드의 데이터는 즉시 삭제되지 않는다. 대신 `Trim` API를 호출받은 스토리지 노드는 `until` 로 지정된 로그 위치를 삭제된 것이라고 표시하고, 이후에 오는 읽기 요청을 거절한다.

## Alternatives

### Consistent View

`Trim` 이 현재 진행되고 있는 읽기 동작을 방해하지 않게 할 수 있다. 즉, `Read` 와 `Subscribe` 가 Consistent View를 제공하고, `Trim` 에 의해 현재 데이터가 없어지는 현상을 방지할 수 있다. 하지만, Consistent View를 제공하는 것은 스토리지 노드에 부하를 제공할 수 있고, 긴급하게 데이터를 삭제해야하는 것을 방해할 수 있다.

Varlog 에서 제공하는 `Trim` 연산은 로그에 대한 I/O 라기 보다는 운영을 위한 기능이다. 왜냐하면 로그는 immutable하기 때문이다. 그러므로 Varlog는 `Trim` 에 대한 보호를 위한 Consistent View를 제공하지 않는다.

## Open Issues

차후에 구현해야할 `Trim` 관련 기능으로 아래와 같은 것들이 있다.

- MaximumTrimmableBoundary
  - `Trim` 의 `until` 파라미터가 될 수 있는 최대값을 제한한다.
  - `Trim` 의 `until` 파라미터가 가리키는 로그의 생성날짜를 이용하여 최근 데이터 삭제는 방지한다.