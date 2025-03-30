package client

import (
	"context"
	"io"
	"sort"
	"sync"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/snpb/mock"
	"github.com/kakao/varlog/proto/varlogpb"
)

type byGLSN []types.GLSN

func (x byGLSN) Len() int { return len(x) }

func (x byGLSN) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byGLSN) Less(i, j int) bool { return x[i] < x[j] }

type storageNode struct {
	glsn       types.GLSN
	llsn       types.LLSN
	logEntries map[types.GLSN][]byte
	glsnToLLSN map[types.GLSN]types.LLSN
	appendReq  *snpb.AppendRequest
	mu         sync.Mutex
}

func newStorageNode() storageNode {
	return storageNode{
		glsn:       types.GLSN(0),
		llsn:       types.LLSN(0),
		logEntries: make(map[types.GLSN][]byte),
		glsnToLLSN: make(map[types.GLSN]types.LLSN),
	}
}

func newMockStorageNodeServiceClient(ctrl *gomock.Controller, sn *storageNode, tpid types.TopicID, lsid types.LogStreamID) *mock.MockLogIOClient {
	mockClient := mock.NewMockLogIOClient(ctrl)

	// Append
	mockAppendClient := mock.NewMockLogIO_AppendClient(ctrl)
	mockAppendClient.EXPECT().Send(gomock.Any()).DoAndReturn(func(req *snpb.AppendRequest) error {
		sn.mu.Lock()
		defer sn.mu.Unlock()
		sn.appendReq = req
		return nil
	}).AnyTimes()
	mockAppendClient.EXPECT().Recv().DoAndReturn(func() (*snpb.AppendResponse, error) {
		sn.mu.Lock()
		defer sn.mu.Unlock()

		rsp := &snpb.AppendResponse{}
		for _, buf := range sn.appendReq.GetPayload() {
			sn.logEntries[sn.glsn] = buf
			sn.glsnToLLSN[sn.glsn] = sn.llsn
			rsp.Results = append(rsp.Results, snpb.AppendResult{
				Meta: varlogpb.LogEntryMeta{
					TopicID:     tpid,
					LogStreamID: lsid,
					GLSN:        sn.glsn,
					LLSN:        sn.llsn,
				},
			})
			sn.glsn++
			sn.llsn++
		}
		return rsp, nil
	}).AnyTimes()
	mockAppendClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	mockClient.EXPECT().Append(gomock.Any(), gomock.Any()).Return(mockAppendClient, nil).AnyTimes()

	// Subscribe
	mockClient.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, req *snpb.SubscribeRequest, opts ...grpc.CallOption) (snpb.LogIO_SubscribeClient, error) {
		nextGLSN := req.GetGLSNBegin()
		stream := mock.NewMockLogIO_SubscribeClient(ctrl)
		stream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(
			func(m any) error {
				sn.mu.Lock()
				defer sn.mu.Unlock()
				var glsns []types.GLSN
				for glsn := range sn.logEntries {
					glsns = append(glsns, glsn)
				}
				sort.Sort(byGLSN(glsns))
				for _, glsn := range glsns {
					if glsn < nextGLSN {
						continue
					}
					nextGLSN = glsn + 1
					rsp := m.(*snpb.SubscribeResponse)
					rsp.GLSN = glsn
					rsp.LLSN = sn.glsnToLLSN[glsn]
					rsp.Payload = sn.logEntries[glsn]
					return nil
				}
				return io.EOF
			},
		).AnyTimes()
		stream.EXPECT().Recv().DoAndReturn(
			func() (*snpb.SubscribeResponse, error) {
				sn.mu.Lock()
				defer sn.mu.Unlock()
				var glsns []types.GLSN
				for glsn := range sn.logEntries {
					glsns = append(glsns, glsn)
				}
				sort.Sort(byGLSN(glsns))
				for _, glsn := range glsns {
					if glsn < nextGLSN {
						continue
					}
					nextGLSN = glsn + 1
					return &snpb.SubscribeResponse{
						GLSN:    glsn,
						LLSN:    sn.glsnToLLSN[glsn],
						Payload: sn.logEntries[glsn],
					}, nil
				}
				return nil, io.EOF
			},
		).AnyTimes()
		return stream, nil
	}).AnyTimes()

	// TrimDeprecated
	mockClient.EXPECT().TrimDeprecated(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, req *snpb.TrimDeprecatedRequest, opts ...grpc.CallOption) (*pbtypes.Empty, error) {
		sn.mu.Lock()
		defer sn.mu.Unlock()
		var num uint64 = 0
		for glsn := range sn.logEntries {
			if glsn > req.GetGLSN() {
				continue
			}
			delete(sn.logEntries, glsn)
			delete(sn.glsnToLLSN, glsn)
			num++
		}
		return &pbtypes.Empty{}, nil
	})

	return mockClient
}

func TestBasicOperations(t *testing.T) {
	const logStreamID = types.LogStreamID(0)
	const topicID = types.TopicID(1)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sn := newStorageNode()
	mockClient := newMockStorageNodeServiceClient(ctrl, &sn, topicID, logStreamID)

	client := &LogClient{rpcClient: mockClient}
	Convey("Simple Append/Read/Subscribe/TrimDeprecated operations should work", t, func() {
		var prevGLSN types.GLSN
		var currGLSN types.GLSN
		var res []snpb.AppendResult
		var err error
		var msg string

		msg = "msg-1"
		res, err = client.Append(context.TODO(), topicID, logStreamID, [][]byte{[]byte(msg)})
		currGLSN = res[0].Meta.GLSN
		So(err, ShouldBeNil)
		prevGLSN = currGLSN

		msg = "msg-2"
		res, err = client.Append(context.TODO(), topicID, logStreamID, [][]byte{[]byte(msg)})
		currGLSN = res[0].Meta.GLSN
		So(err, ShouldBeNil)
		So(currGLSN, ShouldBeGreaterThan, prevGLSN)

		ch, err := client.Subscribe(context.TODO(), topicID, logStreamID, types.GLSN(0), types.GLSN(10))
		So(err, ShouldBeNil)
		subRes := <-ch
		So(subRes.Error, ShouldBeNil)
		So(subRes.GLSN, ShouldEqual, types.GLSN(0))
		So(subRes.LLSN, ShouldEqual, types.LLSN(0))
		So(string(subRes.Data), ShouldEqual, "msg-1")

		subRes = <-ch
		So(subRes.Error, ShouldBeNil)
		So(subRes.GLSN, ShouldEqual, types.GLSN(1))
		So(subRes.LLSN, ShouldEqual, types.LLSN(1))
		So(string(subRes.Data), ShouldEqual, "msg-2")

		// drain channel to avoid leak of goroutine
		for range ch {
		}

		err = client.TrimDeprecated(context.TODO(), topicID, types.GLSN(0))
		So(err, ShouldBeNil)
	})
}
