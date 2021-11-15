package varlogtest

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type testLog struct {
	vt *VarlogTest
}

var _ varlog.Log = (*testLog)(nil)

func (c *testLog) lock() error {
	c.vt.mu.Lock()
	if c.vt.adminClientClosed {
		c.vt.mu.Unlock()
		return errors.New("closed")
	}
	return nil
}

func (c *testLog) unlock() {
	c.vt.mu.Unlock()
}

func (c *testLog) Close() error {
	c.vt.mu.Lock()
	defer c.vt.mu.Unlock()
	c.vt.varlogClientClosed = true
	return nil
}

func (c *testLog) Append(ctx context.Context, topicID types.TopicID, data []byte, opts ...varlog.AppendOption) (types.GLSN, error) {
	if err := c.lock(); err != nil {
		return types.InvalidGLSN, err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return types.InvalidGLSN, errors.New("no such topic")
	}
	if len(topicDesc.LogStreams) == 0 {
		return types.InvalidGLSN, errors.New("no log stream")
	}

	logStreamID := topicDesc.LogStreams[c.vt.rng.Intn(len(topicDesc.LogStreams))]

	return c.appendTo(topicID, logStreamID, data)
}

func (c *testLog) AppendTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data []byte, opts ...varlog.AppendOption) (types.GLSN, error) {
	if err := c.lock(); err != nil {
		return types.InvalidGLSN, err
	}
	defer c.unlock()

	return c.appendTo(topicID, logStreamID, data)
}

func (c *testLog) appendTo(topicID types.TopicID, logStreamID types.LogStreamID, data []byte) (types.GLSN, error) {
	topicDesc, err := c.topicDescriptor(topicID)
	if err != nil {
		return types.InvalidGLSN, err
	}
	if !topicDesc.HasLogStream(logStreamID) {
		return types.InvalidGLSN, errors.New("no such log stream in the topic")
	}

	if _, ok := c.vt.localLogEntries[logStreamID]; !ok {
		return types.InvalidGLSN, errors.New("no such log stream")
	}

	lastGIdx := len(c.vt.globalLogEntries[topicID]) - 1
	lastGLSN := c.vt.globalLogEntries[topicID][lastGIdx].GLSN

	lastLIdx := len(c.vt.localLogEntries[logStreamID]) - 1
	lastLLSN := c.vt.localLogEntries[logStreamID][lastLIdx].LLSN

	logEntry := &varlogpb.LogEntry{
		GLSN: lastGLSN + 1,
		LLSN: lastLLSN,
		Data: make([]byte, len(data)),
	}
	copy(logEntry.Data, data)

	c.vt.globalLogEntries[topicID] = append(c.vt.globalLogEntries[topicID], logEntry)
	c.vt.localLogEntries[logStreamID] = append(c.vt.localLogEntries[logStreamID], logEntry)

	return logEntry.GLSN, nil
}

func (c *testLog) topicDescriptor(topicID types.TopicID) (varlogpb.TopicDescriptor, error) {
	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return varlogpb.TopicDescriptor{}, errors.New("no such topic")
	}
	if len(topicDesc.LogStreams) == 0 {
		return varlogpb.TopicDescriptor{}, errors.New("no log stream")
	}
	return topicDesc, nil
}

func (c *testLog) Read(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, glsn types.GLSN) ([]byte, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	topicDesc, err := c.topicDescriptor(topicID)
	if err != nil {
		return nil, err
	}
	if !topicDesc.HasLogStream(logStreamID) {
		return nil, errors.New("no such log stream in the topic")
	}

	n := len(c.vt.globalLogEntries[topicID])
	if c.vt.globalLogEntries[topicID][n-1].GLSN < glsn {
		// NOTE: This differs from the real varlog.
		return nil, errors.New("no such log entry")
	}
	data := make([]byte, len(c.vt.globalLogEntries[topicID][glsn].Data))
	copy(data, c.vt.globalLogEntries[topicID][glsn].Data)
	return data, nil
}

func (c *testLog) Subscribe(ctx context.Context, topicID types.TopicID, begin types.GLSN, end types.GLSN, onNextFunc varlog.OnNext, opts ...varlog.SubscribeOption) (varlog.SubscribeCloser, error) {
	if begin >= end {
		return nil, errors.New("invalid range")
	}

	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	_, err := c.topicDescriptor(topicID)
	if err != nil {
		return nil, err
	}

	logEntries := c.vt.globalLogEntries[topicID]
	n := len(logEntries)
	if logEntries[n-1].GLSN < begin {
		// NOTE: This differs from the real varlog.
		return nil, errors.New("no such log entry")
	}

	if begin.Invalid() {
		begin = types.MinGLSN
	}
	if end > logEntries[n-1].GLSN {
		end = logEntries[n-1].GLSN + 1
	}

	copiedLogEntries := make([]varlogpb.LogEntry, 0, end-begin)
	for glsn := begin; glsn < end; glsn++ {
		logEntry := varlogpb.LogEntry{
			GLSN: glsn,
			LLSN: logEntries[glsn].LLSN,
			Data: make([]byte, len(logEntries[glsn].Data)),
		}
		copy(logEntry.Data, logEntries[glsn].Data)
		copiedLogEntries = append(copiedLogEntries, logEntry)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, logEntry := range copiedLogEntries {
			onNextFunc(logEntry, nil)
		}
		onNextFunc(varlogpb.InvalidLogEntry(), io.EOF)
	}()

	return func() {
		wg.Wait()
	}, nil
}

func (c *testLog) Trim(ctx context.Context, topicID types.TopicID, until types.GLSN, opts varlog.TrimOption) error {
	panic("not implemented")
}
