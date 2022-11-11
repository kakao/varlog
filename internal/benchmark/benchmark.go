package benchmark

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kakao/varlog/pkg/varlog"
)

type appendStat struct {
	success    int64
	failure    int64
	totalBytes int64
	startTime  time.Time
	durations  int64
}

func newAppendStat() *appendStat {
	return &appendStat{
		startTime: time.Now(),
	}
}

func (as *appendStat) addSuccess(success int64) {
	atomic.AddInt64(&as.success, success)
}

func (as *appendStat) addFailure(failure int64) {
	atomic.AddInt64(&as.failure, failure)
}

func (as *appendStat) elapsedTime() float64 {
	return time.Since(as.startTime).Seconds()
}

func (as *appendStat) addResponseTime(responseTime time.Duration) {
	atomic.AddInt64(&as.durations, responseTime.Milliseconds())
}

func (as *appendStat) throughput() float64 {
	duration := as.elapsedTime()
	if duration == 0 {
		return 0
	}
	return float64(as.totalBytes) / duration / 1024.0 / 1024.0
}

func (as *appendStat) responseTime() float64 {
	return float64(as.durations) / float64(as.success)
}

func (as *appendStat) addBytes(byteSize int64) {
	as.totalBytes += byteSize
}

func (as *appendStat) printHeader(w io.Writer) {
	fmt.Fprintln(w, "success\tfailure\tMB\telapsed_time(s)\tthroughput(MB/s)\tresponse_time(ms)")
}

func (as *appendStat) printStat(w io.Writer) {
	fmt.Fprintf(w, "%d\t%d\t%f\t%f\t%f\t%f\n", as.success, as.failure, float64(as.totalBytes)/float64(1<<20), as.elapsedTime(), as.throughput(), as.responseTime())
}

func Append(opts ...Option) error {
	cfg, err := newConfig(opts)
	if err != nil {
		return err
	}

	batch := make([][]byte, cfg.batchSize)
	for i := 0; i < cfg.batchSize; i++ {
		msg := make([]byte, cfg.msgSize)
		for j := 0; j < cfg.msgSize; j++ {
			msg[j] = '.'
		}
		batch[i] = msg
	}
	payloadBytes := int64(cfg.msgSize * cfg.batchSize)

	newAppendStat().printHeader(os.Stderr)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	var reportWg sync.WaitGroup
	stat := newAppendStat()
	reportWg.Add(1)
	go func() {
		timer := time.NewTimer(cfg.reportInterval)
		defer func() {
			timer.Stop()
			reportWg.Done()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				stat.printStat(os.Stderr)
				timer.Reset(cfg.reportInterval)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(cfg.concurrency)
	for i := 0; i < cfg.concurrency; i++ {
		go func() {
			defer wg.Done()
			vlog, err := varlog.Open(ctx, cfg.cid, cfg.mraddrs, varlog.WithGRPCDialOptions(
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithReadBufferSize(1<<20),
				grpc.WithWriteBufferSize(32<<20),
			))
			if err != nil {
				panic(err)
			}
			defer func() {
				_ = vlog.Close()
			}()
			for ctx.Err() == nil {
				before := time.Now()
				res := vlog.AppendTo(ctx, cfg.tpid, cfg.lsid, batch)
				if res.Err != nil {
					stat.addFailure(1)
					continue
				}
				stat.addResponseTime(time.Since(before))
				stat.addSuccess(1)
				stat.addBytes(payloadBytes)
			}
		}()
	}
	wg.Wait()
	cancel()
	reportWg.Wait()

	stat.printStat(os.Stderr)
	return nil
}
