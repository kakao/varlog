package varlog

import (
	"testing"

	_ "github.com/kakao/varlog/internal/vtesting"
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
