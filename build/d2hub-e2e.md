# E2E Test & d2hub

PR 요청 하면 자동으로 e2e 테스트를 실행한다.

기존 Jenkins 파이프라인에 e2e 테스트를 위한 단계를 추가했다. e2e 테스트는 `varlog-dev-e2e` 클러스터에서 실행되며 동시에 하나의 테스트만 실행할 수 있다. 이는 Jenkins의 `Lockable Resources`  기능을 사용했다.

```
                                                                                  
     +-------------+     +-------------+     +-------------+      +--------------+
     |  developer  |     |   github    |     |    d2hub    |      |varlog-dev-e2e|
     +-------------+     +-------------+     +-------------+      +--------------+
            |                   |                   |                     |       
            +----pull request--->                   |                     |       
            |                   |                   |                     |       
            |                  +++                  |                     |       
            |                 build                 |                     |       
            |                  | |                  |                     |       
            |                  +++                  |                     |       
            |                   |                   |                     |       
            |                  +++                  |                     |       
            |                  test                 |                     |       
            |                  | |                  |                     |       
            |                  +++                  |                     |       
            |                   |                   |                     |       
            |                  +++                  |                     |       
            |                  | +-check all-in-one->                     |       
            |                  | |        .         |                     |       
            |                  | |        .         |                     |       
            |                  | |        .         |                     |       
            |                  | +-check all-in-one->                     |       
            |                  +++                  |                     |       
            |                   |                   |                     |       
            |                  +++                  |                     |       
            |               kustomize               |                     |       
            |                  | |                  |                     |       
            |                  +++                  |                     |       
            |                   +----kubectl apply ...-------------------->       
            |                   |                   |                     |       
            |                   +----make test_e2e--+--------------------->       
            |                   |                   |                     |       
     +-------------+     +-------------+     +-------------+      +--------------+
     |  developer  |     |   github    |     |    d2hub    |      |varlog-dev-e2e|
     +-------------+     +-------------+     +-------------+      +--------------+
```
