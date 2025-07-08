package varlog

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/kakao/varlog/internal/storagenode/client"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func BenchmarkAggregationBuffer(b *testing.B) {
	sizes := []int{1 << 7, 1 << 10, 1 << 13}
	tcs := []struct {
		generate func(int) *aggregationBuffer
		name     string
	}{
		{
			name: "NoAlloc",
			generate: func(int) *aggregationBuffer {
				return &aggregationBuffer{
					pq: &PriorityQueue{},
				}
			},
		},
		{
			name: "PreAlloc",
			generate: func(size int) *aggregationBuffer {
				return &aggregationBuffer{
					pq: newPriorityQueue(size / 2),
				}
			},
		},
	}

	for _, tc := range tcs {
		for _, size := range sizes {
			name := fmt.Sprintf("%s/%d", tc.name, size)
			b.Run(name, func(b *testing.B) {
				b.ResetTimer()
				for range b.N {
					tq := tc.generate(size)
					for range size {
						tr := aggregationItem{
							result: client.SubscribeResult{
								LogEntry: varlogpb.LogEntry{
									LogEntryMeta: varlogpb.LogEntryMeta{
										GLSN: types.GLSN(rand.Int31()),
									},
								},
							},
						}
						tq.Push(tr)
					}
				}
			})
		}
	}
}
