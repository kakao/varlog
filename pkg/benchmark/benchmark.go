package benchmark

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/pkg/varlog"
)

type Benchmark interface {
	Run() error
	io.Closer
}

type benchmarkImpl struct {
	config

	once sync.Once

	data    []byte
	clients []varlog.Log
	readyC  chan struct{}
	startC  chan struct{}

	cancel context.CancelFunc
	g      *errgroup.Group

	rng *rand.Rand

	rpt *report
}

var _ Benchmark = (*benchmarkImpl)(nil)

func New(ctx context.Context, opts ...Option) (Benchmark, error) {
	cfg := newConfig(opts)
	b := &benchmarkImpl{
		config: cfg,
		readyC: make(chan struct{}, cfg.numClients),
		startC: make(chan struct{}),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
		data:   make([]byte, cfg.dataSizeByte),
		rpt:    newReport(cfg.numClients * cfg.maxOpsPerClient),
	}

	// data
	_, _ = b.rng.Read(b.data)

	// init clients
	for i := 0; i < b.numClients; i++ {
		cl, err := varlog.Open(ctx, b.clusterID, b.metadataRepositoryAddresses, b.varlogOpts...)
		if err != nil {
			return nil, err
		}
		b.clients = append(b.clients, cl)
	}

	return b, nil
}

func (b *benchmarkImpl) Run() (err error) {
	b.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), b.maxDuration)
		b.cancel = cancel
		g, ctx := errgroup.WithContext(ctx)
		b.g = g

		for i := 0; i < b.numClients; i++ {
			idx := i
			g.Go(func() error {
				return b.clientLoop(ctx, idx)
			})
		}

		if err := b.waitForReadiness(ctx); err != nil {
			return
		}
		startTime := time.Now()
		b.notifyStart()
		err = g.Wait()
		b.rpt.duration = time.Since(startTime)
	})
	return err
}

func (b *benchmarkImpl) Close() (err error) {
	b.cancel()
	err = b.g.Wait()
	for _, client := range b.clients {
		err = multierr.Append(err, client.Close())
	}
	b.report()
	return err
}

func (b *benchmarkImpl) report() {
	fmt.Println(b.rpt.String())
}

func (b *benchmarkImpl) waitForReadiness(ctx context.Context) error {
	for i := 0; i < b.numClients; i++ {
		select {
		case <-b.readyC:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *benchmarkImpl) notifyReadiness() {
	b.readyC <- struct{}{}
}

func (b *benchmarkImpl) waitForStart(ctx context.Context) error {
	select {
	case <-b.startC:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (b *benchmarkImpl) notifyStart() {
	close(b.startC)
}

func (b *benchmarkImpl) clientLoop(ctx context.Context, idx int) error {
	client := b.clients[idx]

	records := make([]record, 0, b.maxOpsPerClient)

	defer func() {
		b.rpt.mu.Lock()
		b.rpt.records = append(b.rpt.records, records...)
		b.rpt.mu.Unlock()
	}()

	b.notifyReadiness()
	if err := b.waitForStart(ctx); err != nil {
		return err
	}

	for i := 1; i <= b.maxOpsPerClient; i++ {
		begin := time.Now()
		_, err := client.Append(ctx, 0, [][]byte{b.data})
		end := time.Now()
		records = append(records, record{
			err:          err,
			responseTime: end.Sub(begin).Seconds(),
			ts:           end,
		})
	}
	return nil
}
