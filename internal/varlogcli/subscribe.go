package varlogcli

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

const (
	subscribeTimeout = 5 * time.Second
)

func Subscribe(mrAddrs []string, clusterID types.ClusterID, topicID types.TopicID) error {
	const size = 10

	vlog, err := open(mrAddrs, clusterID)
	if err != nil {
		return err
	}
	defer func() {
		_ = vlog.Close()
	}()

	for begin := types.MinGLSN; begin < types.MaxGLSN; begin += size {
		if err := subscribe(vlog, topicID, begin, begin+size); err != nil {
			return err
		}
	}
	return nil
}

func subscribe(vlog varlog.Log, topicID types.TopicID, begin, end types.GLSN) error {
	errC := make(chan error)
	onNext := func(logEntry varlogpb.LogEntry, err error) {
		if err != nil {
			errC <- err
			close(errC)
			return
		}
		log.Printf("Subscribe: %s (%+v)", string(logEntry.Data), logEntry)
	}

	ctx, cancel := context.WithTimeout(context.Background(), subscribeTimeout)
	defer cancel()
	closer, err := vlog.Subscribe(ctx, topicID, begin, end, onNext)
	if err != nil {
		return errors.WithMessage(err, "could not subscribe")
	}
	defer closer()
	select {
	case err = <-errC:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return errors.WithMessage(err, "could not subscribe")
}

func SubscribeTo(mrAddrs []string, clusterID types.ClusterID, topicID types.TopicID, logStreamID types.LogStreamID) (err error) {
	vlog, err := open(mrAddrs, clusterID)
	if err != nil {
		return err
	}
	defer func() {
		err = multierr.Append(err, vlog.Close())
	}()

	subscriber := vlog.SubscribeTo(context.Background(), topicID, logStreamID, types.MinLLSN, types.MaxLLSN)
	defer func() {
		err = multierr.Append(err, subscriber.Close())
	}()

	var logEntry varlogpb.LogEntry
	for logEntry, err = subscriber.Next(); err == nil; logEntry, err = subscriber.Next() {
		if err != nil {
			break
		}
		log.Printf("SubscribeTo: %s (%+v)", string(logEntry.Data), logEntry)
	}
	if err != nil && errors.Is(err, io.EOF) {
		err = nil
	}
	return errors.WithMessage(err, "subscribe error")
}
