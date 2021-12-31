package varlogcli

import (
	"bufio"
	"context"
	"log"
	"os"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

// TODO: Use configured value
const (
	openTimeout   = 10 * time.Second
	appendTimeout = 5 * time.Second
)

type appendFunc func(vlog varlog.Log, batch [][]byte) error

func Append(mrAddrs []string, clusterID types.ClusterID, topicID types.TopicID, batchSize int) error {
	f := func(vlog varlog.Log, batch [][]byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
		defer cancel()
		res := vlog.Append(ctx, topicID, batch)
		return res.Err
	}

	return appendInternal(mrAddrs, clusterID, batchSize, f)
}
func AppendTo(mrAddrs []string, clusterID types.ClusterID, topicID types.TopicID, logStreamID types.LogStreamID, batchSize int) error {
	f := func(vlog varlog.Log, batch [][]byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
		defer cancel()
		res := vlog.AppendTo(ctx, topicID, logStreamID, batch)
		return res.Err
	}

	return appendInternal(mrAddrs, clusterID, batchSize, f)
}

func appendInternal(mrAddrs []string, clusterID types.ClusterID, batchSize int, f appendFunc) error {
	vlog, err := open(mrAddrs, clusterID)
	if err != nil {
		return err
	}
	defer func() {
		_ = vlog.Close()
	}()

	ch := make(chan []byte, batchSize)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		total := 0
		defer func() {
			log.Printf("appended messages=%d", total)
		}()

		batch := make([][]byte, 0, batchSize)
	Loop:
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					break Loop
				}
				batch = append(batch, msg)

				if len(batch) < batchSize {
					continue
				}

				if err := f(vlog, batch); err != nil {
					return errors.WithMessage(err, "could not append")
				}
				total += len(batch)
				batch = batch[0:0]
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		if len(batch) > 0 {
			if err := f(vlog, batch); err != nil {
				return errors.WithMessage(err, "could not append")
			}
			total += len(batch)
		}
		return nil
	})
	g.Go(func() error {
		return scanLoop(ctx, vlog, ch)
	})

	return g.Wait()
}

func open(mrAddrs []string, clusterID types.ClusterID) (varlog.Log, error) {
	ctx, cancel := context.WithTimeout(context.Background(), openTimeout)
	defer cancel()
	return varlog.Open(ctx, clusterID, mrAddrs)
}

func scanLoop(ctx context.Context, vlog varlog.Log, ch chan<- []byte) error {
	defer close(ch)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		select {
		case ch <- []byte(text):
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := scanner.Err(); err != nil {
			return errors.WithMessage(err, "could not scan")
		}
	}
	return scanner.Err()
}
