package jobqueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type testQueueCreator struct {
	name   string
	create func(queueSize int) (JobQueue, error)
}

func newTestJobQueueCreators() []testQueueCreator {
	benchmarkQueueCreators := []testQueueCreator{
		{
			name:   "channelQueue",
			create: NewChQueue,
		},
	}
	return benchmarkQueueCreators
}

func BenchmarkQueuePush(b *testing.B) {
	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		b.Run(creator.name, func(b *testing.B) {
			queue, err := creator.create(b.N)
			require.NoError(b, err)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := queue.PushWithContext(context.TODO(), i)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkQueuePop(b *testing.B) {
	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		b.Run(creator.name, func(b *testing.B) {
			queue, err := creator.create(b.N)
			require.NoError(b, err)
			for i := 0; i < b.N; i++ {
				err := queue.PushWithContext(context.TODO(), i)
				require.NoError(b, err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = queue.Pop()
			}
			b.StopTimer()
		})
	}
}

func BenchmarkQueueConcurrentPush(b *testing.B) {
	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		b.Run(creator.name, func(b *testing.B) {
			queue, err := creator.create(b.N)
			require.NoError(b, err)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if err := queue.PushWithContext(context.TODO(), i); err != nil {
						b.Error(err)
						return
					}
					i++
				}
			})
			b.StopTimer()
		})
	}
}

func BenchmarkQueueConcurrentPop(b *testing.B) {
	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		b.Run(creator.name, func(b *testing.B) {
			queue, err := creator.create(b.N)
			require.NoError(b, err)
			for i := 0; i < b.N; i++ {
				err := queue.PushWithContext(context.TODO(), i)
				require.NoError(b, err)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = queue.Pop()
				}
			})
			b.StopTimer()
		})
	}
}

func BenchmarkQueuePushPop(b *testing.B) {
	queueSizes := []int{1, 10, 100, 1000, 10000}

	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		for _, queueSize := range queueSizes {
			name := fmt.Sprintf("%s-queueSize=%d", creator.name, queueSize)
			b.Run(name, func(b *testing.B) {
				queue, err := creator.create(b.N)
				require.NoError(b, err)
				wg := sync.WaitGroup{}
				wg.Add(2)
				b.ResetTimer()
				go func() {
					defer wg.Done()
					for i := 0; i < b.N; i++ {
						if err := queue.PushWithContext(context.TODO(), i); err != nil {
							b.Error(err)
							return
						}
					}
				}()
				go func() {
					defer wg.Done()
					for i := 0; i < b.N; i++ {
						_ = queue.Pop()
					}
				}()
				wg.Wait()
				b.StopTimer()
			})
		}
	}
}

func TestJobQueueVariousSize(t *testing.T) {
	const numItems = 10
	defer goleak.VerifyNone(t)

	testCases := []struct {
		queueSize int
		created   bool
	}{
		{
			queueSize: -1,
			created:   false,
		},
		{
			queueSize: 0,
			created:   false,
		},
		{
			queueSize: 1,
			created:   true,
		},
	}

	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		for _, tc := range testCases {
			tc := tc
			name := fmt.Sprintf("%s-queueSize=%d", creator.name, tc.queueSize)
			t.Run(name, func(t *testing.T) {
				queue, err := creator.create(tc.queueSize)
				if !tc.created {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				// defer queue.Close()

				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < numItems; i++ {
						err = queue.PushWithContext(context.TODO(), i)
						require.NoError(t, err)
					}
				}()
				for i := 0; i < numItems; i++ {
					item := queue.Pop()
					assert.Equal(t, i, item)
				}

				assert.Zero(t, queue.Size())
			})
		}
	}
}

func TestJobQueueContextError(t *testing.T) {
	const queueSize = 10
	defer goleak.VerifyNone(t)

	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		t.Run(creator.name, func(t *testing.T) {
			queue, err := creator.create(queueSize)
			require.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(2)
			ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Microsecond)
			defer cancel()
			go func() {
				defer wg.Done()
				item := 0
				for {
					if err := queue.PushWithContext(ctx, item); err != nil {
						return
					}
					item++
				}
			}()
			go func() {
				defer wg.Done()
				for {
					if _, err := queue.PopWithContext(ctx); err != nil {
						return
					}
				}
			}()
			wg.Wait()
		})
	}
}

func TestJobQueueClose(t *testing.T) {
	const (
		queueSize   = 10
		pushedItems = int32(1000)
	)
	defer goleak.VerifyNone(t)

	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		t.Run(creator.name, func(t *testing.T) {
			queue, err := creator.create(queueSize)
			require.NoError(t, err)

			pushWg := sync.WaitGroup{}
			pushWg.Add(1)
			go func() {
				defer pushWg.Done()
				for item := int32(0); item < pushedItems; item++ {
					err := queue.PushWithContext(context.TODO(), item)
					require.NoError(t, err)
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			popWg := sync.WaitGroup{}
			popWg.Add(1)
			poppedItems := int32(0)
			go func() {
				defer popWg.Done()
				for {
					_, err := queue.PopWithContext(ctx)
					if err != nil {
						return
					}
					atomic.AddInt32(&poppedItems, 1)
				}
			}()
			require.Eventually(t, func() bool {
				return pushedItems == atomic.LoadInt32(&poppedItems)
			}, time.Second, 10*time.Millisecond)

			cancel()

			pushWg.Wait()
			popWg.Wait()

			require.Zero(t, queue.Size())
		})
	}
}

func TestJobQueueSize(t *testing.T) {
	const queueSize = 10
	defer goleak.VerifyNone(t)

	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		t.Run(creator.name, func(t *testing.T) {
			queue, err := creator.create(queueSize)
			require.NoError(t, err)

			for i := 0; i < queueSize; i++ {
				err := queue.PushWithContext(context.TODO(), i)
				require.NoError(t, err)
			}
			assert.Equal(t, queueSize, queue.Size())
		})
	}
}

func TestJobQueueNilItem(t *testing.T) {
	const queueSize = 10
	defer goleak.VerifyNone(t)

	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		t.Run(creator.name, func(t *testing.T) {
			queue, err := creator.create(queueSize)
			require.NoError(t, err)

			err = queue.PushWithContext(context.TODO(), nil)
			assert.NoError(t, err)

			item := queue.Pop()
			assert.Nil(t, item)
		})
	}
}

func TestJobQueueFIFO(t *testing.T) {
	const queueSize = 10
	defer goleak.VerifyNone(t)

	benchmarkQueueCreators := newTestJobQueueCreators()
	for _, creator := range benchmarkQueueCreators {
		creator := creator
		t.Run(creator.name, func(t *testing.T) {
			queue, err := creator.create(queueSize)
			require.NoError(t, err)

			for i := 0; i < queueSize; i++ {
				err = queue.PushWithContext(context.TODO(), i)
				assert.NoError(t, err)
			}

			for i := 0; i < queueSize; i++ {
				item := queue.Pop()
				require.Equal(t, i, item)
			}
		})
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
