# Report and Commit of Log Stream

LS 는 클라이언트가 Append 하는 로그를 디스크에 Write 한다. 하지만 클라이언트에게 Append 된 로그 위치를 응답하기 위해서는 written but uncommitted 로그가 Commit 되어야 한다. Commit 은 LS 스스로 할 수 없고 MR 에 의해 결정된다.

SN 의 여러 LS 들에 쓰여진 uncommitted 로그에 대한 정보를 MR 에 알리는 과정을 *Report* 라 한다. MR 이 Report 를 바탕으로 판단한 로그의 위치 정보 (a.k.a, GLSN) 를 SN 에게 전달하는 과정을 *Commit* 이라 한다.

       +----+                        
       | SN |                        
       +----+                        
             ^                       
              \ report/commit        
               \-------------\       
                              v      
       +----+   report/commit  +----+
       | SN |<---------------->| MR |
       +----+                  +----+
                              ^      
               /-------------/       
              / report/commit        
             v                       
       +----+                        
       | SN |                        
       +----+                        

GLSN이 부여된 로그는 클라이언트에게 visible 하게 되어, Read 나 Subscribe 할 수 있고 SN의 failure가 일어나도 durable하다. 반대로 GLSN이 부여되지 않은 로그는 아직 uncommitted 이며, durable 하지 않고 클라이언트에게 visible하지 않다.

## Report

Report 는 LS 의 로그 저장 상태를 MR 에게 알리는 과정이다. LS 에 저장된 로그는 항상 아래와 같다. 로그의 앞 부분에는 MR 로부터 GLSN을 부여 받은 committed 로그가 있고, 뒷 부분에는 아직 GLSN을 부여 받지 못한 uncommitted 로그가 있다. LS 는 committed & uncommitted 로그를 관리하기 위해 내부의 로그 위치를 나타내는 LLSN 을 관리한다. *uncommitted offset* 은 uncommitted 로그의 시작 지점이며 LLSN 으로 표현한다. *uncommitted length* 는 uncommitted 로그의 수이다. *global high watermark* 는 offset 바로 이전의 committed 로그가 GLSN을 부여받을 때, Varlog 클러스터 전체의 가장 높은 GLSN 이다. 즉, high watermark 는 offset 에 있는 uncommitted 로그가 쓰이기 이전의 클러스터 전체 상태를 나타내는 값이다.


                      global high watermark            
                                       |               
                                       v               
     +---------------------------------+--------------+
     |            committed            | uncommitted  |
     +---------------------------------+--------------+
                                       ^              |
                                       +-uncommitted ->
                                       |    length    |
                        uncommitted offset                                  

Report 는 LS 의 로그 저장 상태를 나타내지만, SN 은 자신에게 저장된 여러 LS 의 상태를 묶어서 MR 에게 전달한다. 이는 MR 과 LS 의 과도한 자원 사용을 방지한다. 하지만 같은 SN 에 저장된 LS 들이 모두 같은 상태를 갖지 않는다. committed 로그와 uncommitted 로그의 양과 경계가 다르며, 각자가 알고 있는 global high watermark 역시 다르다. 왜냐하면 클라이언트는 랜덤하게 LS 를 선택하여 로그를 Append 하고, 따라서 LS 마다 저장하고 있는 uncommitted 로그의 수가 다르다. 그리고 LS 가 데이터를 저장하고 있는 볼륨의 특성과 상태에 따라 저장하는 속도는 제각각 다르다.

아래 프로토콜 정의는 Report 와 Commit 을 모두 포함한다. 

```protobuf
message LogStreamUncommitReport {
	uint32 log_stream_id = 1;
	uint64 uncommitted_llsn_offset = 2;
	uint64 uncommitted_llsn_length = 3;
	uint64 high_watermark = 4;
}

message GetReportRequest {}

message GetReportResponse {
	uint32 storage_node_id = 1;
	repeated LogStreamUncommitReport uncommit_reports = 2;
}

message LogStreamCommitResult {
	uint32 log_stream_id = 1;
	uint64 committed_glsn_offset = 2;
	uint64 committed_glsn_length = 3;
	uint64 high_watermark = 4;
	uint64 prev_high_watermark = 5;
}

message CommitRequest {
	uint32 storage_node_id = 1;
	repeated LogStreamCommitResult commit_results = 2;
}

message CommitResponse {}

service LogStreamReporterService {
	rpc GetReport(GetReportRequest) returns (GetReportResponse) {}
	rpc Commit(CommitRequest) returns (CommitResponse) {}
}
```

## Commit

Commit 은 LS 에 저장된 uncommitted 로그들에게 GLSN 을 부여하고 committed 로그로 변경시킨다. 


     +---------------------------------+--------------+        
     |            committed            | uncommitted  |        
     +---------------------------------+---------+----+        
                                       |         |             
                                       | Commit  |             
                                       <--------->             
                                       |         |             
     +---------------------------------+---------+------------+
     |            committed            |committed|uncommitted |
     +---------------------------------+---------+------------+
                                                 |            |
                                                 |   Report   |
                                                 <------------>
                                                 |            |
위 그림은 Commit 에 의해 uncommitted log 중 일부가 commit 되어지는 모습이다. 그림과 같이 LS 가 Report 한 uncommitted log 가 모두 commit 되지 않을 수 있으며,  이때는 새로운 global high watermark 와 uncommitted offset, uncommitted length 가 정해진다.

## Sealed Log Stream Replica

기존의 LS 에 새로운 Replica 가 추가되면 해당 LS는 아무런 상태를 갖지 않는다. 그리고 이런 경우 기존의 LS 는 Seal 되어있는 상태이다. 새롭게 추가된 Replica 는 다른 Replica 들과는 다른 Report 를 MR 에게 전달한다. 아무런 상태를 갖지 않는 Replica 의 Report 는 모든 필드가 초기값 (global high watermark  == 0, uncommitted offset == 1, uncommitted length == 0) 이다. MR 은 Seal 되어 있는 LS 의 Replica 중 일부가 global high watermark == 0 인 Report 를 보낸다면, 새롭게 참여한 Replica 라고 알 수 있다.