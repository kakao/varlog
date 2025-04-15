package varlog

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/storagenode/client"
	_ "github.com/kakao/varlog/internal/vtesting"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestSubscribe(t *testing.T) {
	t.Skip()

	//Convey("Given varlog client", t, func() {
	//	const (
	//		numLogStreams  = 10
	//		numLogs        = 100
	//		minLogStreamID = types.LogStreamID(1)
	//		topicID        = types.TopicID(1)
	//	)
	//	var (
	//		begin = types.InvalidGLSN
	//		end   = types.InvalidGLSN
	//	)
	//
	//	ctrl := gomock.NewController(t)
	//	defer ctrl.Finish()
	//
	//	metadataRefresher := NewMockMetadataRefresher(ctrl)
	//	metadataRefresher.EXPECT().Refresh(gomock.Any()).Return().AnyTimes()
	//	metadataRefresher.EXPECT().Metadata().Return(nil).AnyTimes()
	//
	//	replicasRetriever := NewMockReplicasRetriever(ctrl)
	//	replicasMap := make(map[types.LogStreamID][]varlogpb.LogStreamReplicaDescriptor, numLogStreams)
	//	for logStreamID := minLogStreamID; logStreamID < minLogStreamID+numLogStreams; logStreamID++ {
	//		replicasMap[logStreamID] = []varlogpb.LogStreamReplicaDescriptor{
	//			{
	//				StorageNodeID: types.StorageNodeID(logStreamID),
	//				LogStreamID:   logStreamID,
	//				Address:       "127.0.0.1:" + strconv.Itoa(int(logStreamID)),
	//			},
	//		}
	//	}
	//	replicasRetriever.EXPECT().All(topicID).Return(replicasMap).MaxTimes(1)
	//
	//	createMockLogClientManager := func(results map[types.LogStreamID][]logclient.SubscribeResult) *logclient.MockLogClientManager {
	//		logCLManager := logclient.NewMockLogClientManager(ctrl)
	//		logCLManager.EXPECT().GetOrConnect(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
	//			func(_ context.Context, storageNodeID types.StorageNodeID, addr string) (logclient.LogIOClient, error) {
	//				logCL := logclient.NewMockLogIOClient(ctrl)
	//				logCL.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ types.TopicID, logStreamID types.LogStreamID, _ types.GLSN, _ types.GLSN) (<-chan logclient.SubscribeResult, error) {
	//					result := results[logStreamID]
	//					c := make(chan logclient.SubscribeResult, len(result))
	//					for _, res := range result {
	//						c <- res
	//					}
	//					close(c)
	//					return c, nil
	//				})
	//				return logCL, nil
	//			},
	//		).MaxTimes(numLogStreams)
	//		return logCLManager
	//	}
	//
	//	vlg := &logImpl{}
	//	vlg.logger = zap.L()
	//	vlg.runner = runner.New("varlog-test", zap.L())
	//	vlg.replicasRetriever = replicasRetriever
	//	vlg.refresher = metadataRefresher
	//
	//	Convey("When begin >= end", func() {
	//		begin = types.GLSN(2)
	//		end = types.GLSN(1)
	//
	//		Convey("Then subscribe should return an error", func() {
	//			_, err := vlg.subscribe(context.TODO(), topicID, begin, end, func(_ varlogpb.LogEntry, _ error) {})
	//			So(err, ShouldNotBeNil)
	//		})
	//	})
	//
	//	Convey("When log streams are empty", func() {
	//		begin = types.GLSN(1)
	//		end = begin + types.GLSN(numLogs)
	//
	//		results := make(map[types.LogStreamID][]logclient.SubscribeResult, numLogStreams)
	//		for logStreamID := minLogStreamID; logStreamID < minLogStreamID+numLogStreams; logStreamID++ {
	//			results[logStreamID] = nil
	//		}
	//		vlg.logCLManager = createMockLogClientManager(results)
	//
	//		Convey("Then subscribe should work well", func() {
	//			var wg sync.WaitGroup
	//			wg.Add(1)
	//			onNext := func(logEntry varlogpb.LogEntry, err error) {
	//				if err == io.EOF {
	//					wg.Done()
	//					return
	//				}
	//				t.Error("no log entries are expected")
	//			}
	//			closer, err := vlg.subscribe(context.TODO(), topicID, begin, end, onNext)
	//			So(err, ShouldBeNil)
	//			wg.Wait()
	//			closer()
	//		})
	//
	//	})
	//
	//	Convey("When log streams have log entries", func() {
	//		begin = types.GLSN(1)
	//		end = begin + types.GLSN(numLogs)
	//		results := make(map[types.LogStreamID][]logclient.SubscribeResult, numLogStreams)
	//		lastLLSNs := make(map[types.LogStreamID]types.LLSN, numLogStreams)
	//		for glsn := begin; glsn < end; glsn++ {
	//			logStreamID := types.LogStreamID(rand.Intn(numLogStreams) + 1)
	//			lastLLSN := lastLLSNs[logStreamID]
	//			lastLLSN++
	//			results[logStreamID] = append(results[logStreamID], logclient.SubscribeResult{
	//				LogEntry: varlogpb.LogEntry{
	//					LogEntryMeta: varlogpb.LogEntryMeta{
	//						GLSN: glsn,
	//						LLSN: lastLLSN,
	//					},
	//					Data: []byte("foo"),
	//				},
	//				Error: nil,
	//			})
	//			lastLLSNs[logStreamID] = lastLLSN
	//		}
	//		vlg.logCLManager = createMockLogClientManager(results)
	//
	//		Convey("Then subscribe should work well", func(c C) {
	//			var wg sync.WaitGroup
	//			wg.Add(1)
	//			expectedGLSN := begin
	//			onNext := func(logEntry varlogpb.LogEntry, err error) {
	//				if err == io.EOF {
	//					wg.Done()
	//					return
	//				}
	//				if err != nil {
	//					t.Error(err)
	//				}
	//				if expectedGLSN != logEntry.GLSN {
	//					t.Errorf("expected (%v) != actual (%v)", expectedGLSN, logEntry.GLSN)
	//				}
	//				if err == nil {
	//					expectedGLSN++
	//				}
	//			}
	//			closer, err := vlg.subscribe(context.TODO(), topicID, begin, end, onNext)
	//			So(err, ShouldBeNil)
	//			wg.Wait()
	//			closer()
	//		})
	//
	//		Convey("Then subscribe which is requested more logs should work well", func() {
	//			const numMoreLogs = 100
	//			var wg sync.WaitGroup
	//			wg.Add(1)
	//			expectedGLSN := begin
	//			onNext := func(logEntry varlogpb.LogEntry, err error) {
	//				if err == io.EOF {
	//					wg.Done()
	//					return
	//				}
	//				if err != nil {
	//					t.Error(err)
	//				}
	//				if expectedGLSN != logEntry.GLSN {
	//					t.Errorf("expected (%v) != actual (%v)", expectedGLSN, logEntry.GLSN)
	//				}
	//				if err == nil {
	//					expectedGLSN++
	//				}
	//			}
	//			closer, err := vlg.subscribe(context.TODO(), topicID, begin, end+numMoreLogs, onNext)
	//			So(err, ShouldBeNil)
	//			wg.Wait()
	//			closer()
	//		})
	//
	//		Convey("Then subscribe which closes in the middle should stop well", func() {
	//			closePoint := (begin + end) / 2
	//			var wg sync.WaitGroup
	//			wg.Add(1)
	//			expectedGLSN := begin
	//			glsnC := make(chan types.GLSN)
	//			onNext := func(logEntry varlogpb.LogEntry, err error) {
	//				if err != nil {
	//					// NOTE: Regardless of context error or EOF, an
	//					// error should be raised only once.
	//					close(glsnC)
	//					wg.Done()
	//					return
	//				}
	//				if expectedGLSN != logEntry.GLSN {
	//					t.Errorf("expected (%v) != actual (%v)", expectedGLSN, logEntry.GLSN)
	//				}
	//				if err == nil {
	//					glsnC <- logEntry.GLSN
	//					expectedGLSN++
	//				}
	//			}
	//			closer, err := vlg.subscribe(context.TODO(), topicID, begin, end, onNext)
	//			So(err, ShouldBeNil)
	//			go func() {
	//				for glsn := range glsnC {
	//					if glsn == closePoint {
	//						closer()
	//					}
	//				}
	//			}()
	//			wg.Wait()
	//		})
	//	})
	//})
}

func TestSubscriberRefresh_Timeout(t *testing.T) {
	const (
		logStreamID      = types.LogStreamID(1)
		minStorageNodeID = types.StorageNodeID(1)
		topicID          = types.TopicID(1)
	)
	var (
		begin = types.MinGLSN
		end   = begin + 100
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metadataRefresher := NewMockMetadataRefresher(ctrl)
	metadataRefresher.EXPECT().Refresh(gomock.Any()).Return().AnyTimes()
	metadataRefresher.EXPECT().Metadata().Return(nil).AnyTimes()

	leaderStorageNodeID := minStorageNodeID
	followerStorageNodeID := minStorageNodeID + 1

	leadersn := storagenode.TestNewRPCServer(t, ctrl, leaderStorageNodeID)
	leadersn.Run()
	defer leadersn.Close()

	followersn := storagenode.TestNewRPCServer(t, ctrl, followerStorageNodeID)
	followersn.Run()
	defer followersn.Close()

	replicasRetriever := NewMockReplicasRetriever(ctrl)
	replicasMap := make(map[types.LogStreamID][]varlogpb.LogStreamReplica, 1)

	replicasMap[logStreamID] = []varlogpb.LogStreamReplica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(leaderStorageNodeID),
				Address:       leadersn.Address(),
			},
			TopicLogStream: varlogpb.TopicLogStream{
				LogStreamID: logStreamID,
			},
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(followerStorageNodeID),
				Address:       followersn.Address(),
			},
			TopicLogStream: varlogpb.TopicLogStream{
				LogStreamID: logStreamID,
			},
		},
	}
	replicasRetriever.EXPECT().All(topicID).Return(replicasMap).AnyTimes()

	leadersn.MockLogIOServer.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) (err error) {
			ctx := stream.Context()
			<-ctx.Done()
			return ctx.Err()
		},
	).Times(1)

	followersn.MockLogIOServer.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(
		func(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) (err error) {
			ctx := stream.Context()
			glsn := req.GLSNBegin
			llsn := types.LLSN(req.GLSNBegin)
			rsp := &snpb.SubscribeResponse{}
		Loop:
			for {
				select {
				case <-ctx.Done():
					err = ctx.Err()
					break Loop
				default:
					if glsn == req.GLSNEnd {
						break Loop
					}

					rsp.GLSN = glsn
					rsp.LLSN = llsn
					err = stream.SendMsg(rsp)
					if err != nil {
						break Loop
					}

					glsn++
					llsn++
				}
			}
			return err
		},
	).Times(1)

	logCLManager, err := client.NewManager[*client.LogClient]()
	require.NoError(t, err)
	defer logCLManager.Close()

	vlg := &logImpl{}
	vlg.logger = zap.L()
	vlg.runner = runner.New("varlog-test", zap.L())
	vlg.replicasRetriever = replicasRetriever
	vlg.refresher = metadataRefresher
	vlg.logCLManager = logCLManager

	expected := begin
	var wg sync.WaitGroup
	wg.Add(1)
	onNext := func(logEntry varlogpb.LogEntry, err error) {
		if err == io.EOF {
			wg.Done()
			return
		}
		require.Equal(t, expected, logEntry.GLSN)
		expected++
	}
	closer, err := vlg.subscribe(context.TODO(), topicID, begin, end, onNext)
	require.NoError(t, err)
	wg.Wait()
	closer()
}

func TestSubscriberRefresh_Reassign(t *testing.T) {
	const (
		logStreamID      = types.LogStreamID(1)
		minStorageNodeID = types.StorageNodeID(1)
		topicID          = types.TopicID(1)
	)
	var (
		begin = types.MinGLSN
		end   = begin + 100
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metadataRefresher := NewMockMetadataRefresher(ctrl)
	metadataRefresher.EXPECT().Refresh(gomock.Any()).Return().AnyTimes()
	metadataRefresher.EXPECT().Metadata().Return(nil).AnyTimes()

	oldStorageNodeID := minStorageNodeID
	newStorageNodeID := minStorageNodeID + 1

	oldsn := storagenode.TestNewRPCServer(t, ctrl, oldStorageNodeID)
	oldsn.Run()
	defer oldsn.Close()

	newsn := storagenode.TestNewRPCServer(t, ctrl, newStorageNodeID)
	newsn.Run()
	defer newsn.Close()

	replicasRetriever := NewMockReplicasRetriever(ctrl)
	replicasMap := make(map[types.LogStreamID][]varlogpb.LogStreamReplica, 1)

	var mu sync.Mutex

	replicasMap[logStreamID] = []varlogpb.LogStreamReplica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(oldStorageNodeID),
				Address:       oldsn.Address(),
			},
			TopicLogStream: varlogpb.TopicLogStream{
				LogStreamID: logStreamID,
			},
		},
	}

	replicasRetriever.EXPECT().All(topicID).DoAndReturn(
		func(_ types.TopicID) map[types.LogStreamID][]varlogpb.LogStreamReplica {
			m := make(map[types.LogStreamID][]varlogpb.LogStreamReplica, 1)
			mu.Lock()

			for logStreamID, entry := range replicasMap {
				m[logStreamID] = entry
			}

			mu.Unlock()

			return m
		}).AnyTimes()

	oldsn.MockLogIOServer.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) (err error) {
			ctx := stream.Context()
			<-ctx.Done()
			return ctx.Err()
		},
	).MinTimes(1)

	newsn.MockLogIOServer.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(
		func(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) (err error) {
			ctx := stream.Context()
			glsn := req.GLSNBegin
			llsn := types.LLSN(req.GLSNBegin)
			rsp := &snpb.SubscribeResponse{}
		Loop:
			for {
				select {
				case <-ctx.Done():
					err = ctx.Err()
					break Loop
				default:
					if glsn == req.GLSNEnd {
						break Loop
					}

					rsp.GLSN = glsn
					rsp.LLSN = llsn
					err = stream.SendMsg(rsp)
					if err != nil {
						break Loop
					}

					glsn++
					llsn++
				}
			}
			return err
		},
	).Times(1)

	logCLManager, err := client.NewManager[*client.LogClient]()
	require.NoError(t, err)
	defer logCLManager.Close()

	vlg := &logImpl{}
	vlg.logger = zap.L()
	vlg.runner = runner.New("varlog-test", zap.L())
	vlg.replicasRetriever = replicasRetriever
	vlg.refresher = metadataRefresher
	vlg.logCLManager = logCLManager

	expected := begin
	var wg sync.WaitGroup
	wg.Add(1)
	onNext := func(logEntry varlogpb.LogEntry, err error) {
		if err == io.EOF {
			wg.Done()
			return
		}
		require.Equal(t, expected, logEntry.GLSN)
		expected++
	}
	closer, err := vlg.subscribe(context.TODO(), topicID, begin, end, onNext)
	require.NoError(t, err)

	// reassign
	time.Sleep(time.Second)
	mu.Lock()

	replicasMap[logStreamID] = []varlogpb.LogStreamReplica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(newStorageNodeID),
				Address:       newsn.Address(),
			},
			TopicLogStream: varlogpb.TopicLogStream{
				LogStreamID: logStreamID,
			},
		},
	}

	mu.Unlock()

	wg.Wait()
	closer()
}
