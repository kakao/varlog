package varlogcli

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
)

func AppendStress(snAddr string, tpid types.TopicID, lsid types.LogStreamID, msgSize, batchSize, concurrency, appendCount int) error {
	cc, err := grpc.Dial(snAddr,
		grpc.WithReadBufferSize(128*1024),
		grpc.WithWriteBufferSize(128*1024),
	)
	if err != nil {
		return err
	}

	batch := make([][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		msg := make([]byte, msgSize)
		for j := 0; j < msgSize; j++ {
			msg[j] = '.'
		}
		batch[i] = msg
	}
	payloadBytes := int64(msgSize * batchSize)

	fmt.Println("okCnt\terrCnt\tduration\ttotalBytes")
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			startTime := time.Now()
			okCnt, errCnt := int64(0), int64(0)

			defer func() {
				duration := time.Since(startTime).Seconds()
				totalBytes := payloadBytes * okCnt
				fmt.Printf("%d\t%d\t%v\t%d\n",
					okCnt, errCnt, duration, totalBytes)
				wg.Done()
			}()

			client := snpb.NewLogIOClient(cc)
			for j := 0; j < appendCount; j++ {
				_, err := client.Append(context.TODO(), &snpb.AppendRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					Payload:     batch,
				})
				if err != nil {
					errCnt++
					continue
				}
				okCnt++
			}
		}()
	}
	wg.Wait()
	return cc.Close()
}
