package logc

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/snpb/mock"
	"github.com/kakao/varlog/proto/varlogpb"
)

type byGLSN []types.GLSN

func (x byGLSN) Len() int           { return len(x) }
func (x byGLSN) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
func (x byGLSN) Less(i, j int) bool { return x[i] < x[j] }

type storageNode struct {
	glsn       types.GLSN
	llsn       types.LLSN
	logEntries map[types.GLSN][]byte
	glsnToLLSN map[types.GLSN]types.LLSN
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
	mockClient.EXPECT().Append(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, req *snpb.AppendRequest) (*snpb.AppendResponse, error) {
		sn.mu.Lock()
		defer func() {
			sn.glsn++
			sn.llsn++
			sn.mu.Unlock()
		}()
		sn.logEntries[sn.glsn] = req.GetPayload()
		sn.glsnToLLSN[sn.glsn] = sn.llsn
		return &snpb.AppendResponse{
			Results: []snpb.AppendResult{
				{
					Meta: varlogpb.LogEntryMeta{
						TopicID:     tpid,
						LogStreamID: lsid,
						GLSN:        sn.glsn,
						LLSN:        sn.llsn,
					},
				},
			},
		}, nil
	}).AnyTimes()

	// Read
	mockClient.EXPECT().Read(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, req *snpb.ReadRequest) (*snpb.ReadResponse, error) {
		sn.mu.Lock()
		defer sn.mu.Unlock()
		data, ok := sn.logEntries[req.GetGLSN()]
		if !ok {
			return nil, fmt.Errorf("no entry")
		}
		return &snpb.ReadResponse{
			Payload: data,
			GLSN:    req.GetGLSN(),
		}, nil
	}).AnyTimes()

	// Subscribe
	mockClient.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, req *snpb.SubscribeRequest) (snpb.LogIO_SubscribeClient, error) {
		nextGLSN := req.GetGLSNBegin()
		stream := mock.NewMockLogIO_SubscribeClient(ctrl)
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

	// Trim
	mockClient.EXPECT().Trim(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, req *snpb.TrimRequest) (*pbtypes.Empty, error) {
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

	client := &logIOClient{rpcClient: mockClient}
	Convey("Simple Append/Read/Subscribe/Trim operations should work", t, func() {
		var prevGLSN types.GLSN
		var currGLSN types.GLSN
		var currLogEntry *varlogpb.LogEntry
		var res []snpb.AppendResult
		var err error
		var msg string

		msg = "msg-1"
		res, err = client.Append(context.TODO(), topicID, logStreamID, [][]byte{[]byte(msg)})
		currGLSN = res[0].Meta.GLSN
		So(err, ShouldBeNil)
		currLogEntry, err = client.Read(context.TODO(), topicID, logStreamID, currGLSN)
		So(err, ShouldBeNil)
		So(string(currLogEntry.Data), ShouldEqual, msg)
		prevGLSN = currGLSN

		msg = "msg-2"
		res, err = client.Append(context.TODO(), topicID, logStreamID, [][]byte{[]byte(msg)})
		currGLSN = res[0].Meta.GLSN
		So(err, ShouldBeNil)
		So(currGLSN, ShouldBeGreaterThan, prevGLSN)
		currLogEntry, err = client.Read(context.TODO(), topicID, logStreamID, currGLSN)
		So(err, ShouldBeNil)
		So(string(currLogEntry.Data), ShouldEqual, msg)
		prevGLSN = currGLSN

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

		err = client.Trim(context.TODO(), topicID, types.GLSN(0))
		So(subRes.Error, ShouldBeNil)

		currLogEntry, err = client.Read(context.TODO(), topicID, logStreamID, types.GLSN(0))
		So(err, ShouldNotBeNil)
	})
}
