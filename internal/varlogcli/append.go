package varlogcli

import (
	"bufio"
	"context"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

// TODO: Use configured value
const (
	openTimeout   = 10 * time.Second
	appendTimeout = 5 * time.Second
)

func Append(mrAddrs []string, clusterID types.ClusterID, topicID types.TopicID) error {
	f := func(vlog varlog.Log, msg []byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
		defer cancel()
		res := vlog.Append(ctx, topicID, [][]byte{msg})
		return res.Err
	}

	return appendInternal(mrAddrs, clusterID, f)
}
func AppendTo(mrAddrs []string, clusterID types.ClusterID, topicID types.TopicID, logStreamID types.LogStreamID) error {
	f := func(vlog varlog.Log, msg []byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
		defer cancel()
		res := vlog.AppendTo(ctx, topicID, logStreamID, [][]byte{msg})
		return res.Err
	}

	return appendInternal(mrAddrs, clusterID, f)
}

func appendInternal(mrAddrs []string, clusterID types.ClusterID, appendFunc func(vlog varlog.Log, msg []byte) error) error {
	vlog, err := open(mrAddrs, clusterID)
	if err != nil {
		return err
	}
	defer func() {
		_ = vlog.Close()
	}()
	return appendLoop(vlog, appendFunc)
}

func open(mrAddrs []string, clusterID types.ClusterID) (varlog.Log, error) {
	ctx, cancel := context.WithTimeout(context.Background(), openTimeout)
	defer cancel()
	return varlog.Open(ctx, clusterID, mrAddrs)
}

func appendLoop(vlog varlog.Log, appendFunc func(vlog varlog.Log, msg []byte) error) error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if err := appendFunc(vlog, []byte(text)); err != nil {
			return errors.WithMessage(err, "send error")
		}
		if err := scanner.Err(); err != nil {
			return errors.WithMessage(err, "scan error")
		}
	}
	return scanner.Err()
}
