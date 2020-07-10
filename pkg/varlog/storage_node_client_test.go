package varlog

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/mock"
	types "github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
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

func newMockStorageNodeServiceClient(ctrl *gomock.Controller, sn *storageNode) *mock.MockStorageNodeServiceClient {
	mockClient := mock.NewMockStorageNodeServiceClient(ctrl)

	// Append
	mockClient.EXPECT().Append(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
		sn.mu.Lock()
		defer func() {
			sn.glsn++
			sn.llsn++
			sn.mu.Unlock()
		}()
		sn.logEntries[sn.glsn] = req.GetPayload()
		sn.glsnToLLSN[sn.glsn] = sn.llsn
		return &pb.AppendResponse{GLSN: sn.glsn}, nil
	}).AnyTimes()

	// Read
	mockClient.EXPECT().Read(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
		sn.mu.Lock()
		defer sn.mu.Unlock()
		data, ok := sn.logEntries[req.GetGLSN()]
		if !ok {
			return nil, fmt.Errorf("no entry")
		}
		return &pb.ReadResponse{
			Payload: data,
			GLSN:    req.GetGLSN(),
		}, nil
	}).AnyTimes()

	// Subscribe
	mockClient.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, req *pb.SubscribeRequest) (pb.StorageNodeService_SubscribeClient, error) {
		nextGLSN := req.GetGLSN()
		stream := mock.NewMockStorageNodeService_SubscribeClient(ctrl)
		stream.EXPECT().Recv().DoAndReturn(
			func() (*pb.SubscribeResponse, error) {
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
					return &pb.SubscribeResponse{
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
	).DoAndReturn(func(_ context.Context, req *pb.TrimRequest) (*pb.TrimResponse, error) {
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
		return &pb.TrimResponse{
			NumTrimmed: num,
		}, nil
	})

	return mockClient
}

func TestBasicOperations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sn := newStorageNode()
	mockClient := newMockStorageNodeServiceClient(ctrl, &sn)

	const logStreamID = types.LogStreamID(0)
	client := &storageNodeClient{rpcClient: mockClient}
	Convey("Simple Append/Read/Subscribe/Trim operations should work", t, func() {
		var prevGLSN types.GLSN
		var currGLSN types.GLSN
		var currLogEntry *LogEntry
		var err error
		var msg string

		msg = "msg-1"
		currGLSN, err = client.Append(context.TODO(), logStreamID, []byte(msg))
		So(err, ShouldBeNil)
		currLogEntry, err = client.Read(context.TODO(), logStreamID, currGLSN)
		So(err, ShouldBeNil)
		So(string(currLogEntry.Data), ShouldEqual, msg)
		prevGLSN = currGLSN

		msg = "msg-2"
		currGLSN, err = client.Append(context.TODO(), logStreamID, []byte(msg))
		So(err, ShouldBeNil)
		So(currGLSN, ShouldBeGreaterThan, prevGLSN)
		currLogEntry, err = client.Read(context.TODO(), logStreamID, currGLSN)
		So(err, ShouldBeNil)
		So(string(currLogEntry.Data), ShouldEqual, msg)
		prevGLSN = currGLSN

		ch, err := client.Subscribe(context.TODO(), types.GLSN(0))
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

		num, err := client.Trim(context.TODO(), types.GLSN(0))
		So(subRes.Error, ShouldBeNil)
		So(num, ShouldEqual, 1)

		currLogEntry, err = client.Read(context.TODO(), logStreamID, types.GLSN(0))
		So(err, ShouldNotBeNil)
	})
}
