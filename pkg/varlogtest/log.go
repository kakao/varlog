package varlogtest

import (
	"context"
	"io"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/varlogpb"
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

func (c *testLog) Append(ctx context.Context, topicID types.TopicID, data []byte, opts ...varlog.AppendOption) (varlogpb.LogEntryMeta, error) {
	if err := c.lock(); err != nil {
		return varlogpb.InvalidLogEntryMeta(), err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return varlogpb.InvalidLogEntryMeta(), errors.New("no such topic")
	}
	if len(topicDesc.LogStreams) == 0 {
		return varlogpb.InvalidLogEntryMeta(), errors.New("no log stream")
	}

	logStreamID := topicDesc.LogStreams[c.vt.rng.Intn(len(topicDesc.LogStreams))]

	return c.appendTo(topicID, logStreamID, data)
}

func (c *testLog) AppendTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data []byte, opts ...varlog.AppendOption) (varlogpb.LogEntryMeta, error) {
	if err := c.lock(); err != nil {
		return varlogpb.InvalidLogEntryMeta(), err
	}
	defer c.unlock()

	return c.appendTo(topicID, logStreamID, data)
}

func (c *testLog) appendTo(topicID types.TopicID, logStreamID types.LogStreamID, data []byte) (varlogpb.LogEntryMeta, error) {
	topicDesc, err := c.topicDescriptor(topicID)
	if err != nil {
		return varlogpb.InvalidLogEntryMeta(), err
	}
	if !topicDesc.HasLogStream(logStreamID) {
		return varlogpb.InvalidLogEntryMeta(), errors.New("no such log stream in the topic")
	}

	if _, ok := c.vt.localLogEntries[logStreamID]; !ok {
		return varlogpb.InvalidLogEntryMeta(), errors.New("no such log stream")
	}

	_, tail := c.peek(topicID, logStreamID)

	logEntry := &varlogpb.LogEntry{
		LogEntryMeta: varlogpb.LogEntryMeta{
			TopicID:     topicID,
			LogStreamID: logStreamID,
			GLSN:        tail.GLSN + 1,
			LLSN:        tail.LLSN + 1,
		},
		Data: make([]byte, len(data)),
	}
	copy(logEntry.Data, data)

	c.vt.globalLogEntries[topicID] = append(c.vt.globalLogEntries[topicID], logEntry)
	c.vt.localLogEntries[logStreamID] = append(c.vt.localLogEntries[logStreamID], logEntry)

	return logEntry.LogEntryMeta, nil
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
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     logEntries[glsn].TopicID,
				LogStreamID: logEntries[glsn].LogStreamID,
				GLSN:        glsn,
				LLSN:        logEntries[glsn].LLSN,
			},
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

func (c *testLog) SubscribeTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.LLSN, onNextFunc varlog.OnNext, opts ...varlog.SubscribeOption) (varlog.SubscribeCloser, error) {
	panic("not implemented")
}

func (c *testLog) Trim(ctx context.Context, topicID types.TopicID, until types.GLSN, opts varlog.TrimOption) error {
	panic("not implemented")
}

func (c *testLog) LogStreamMetadata(_ context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (varlogpb.LogStreamDescriptor, error) {
	if err := c.lock(); err != nil {
		return varlogpb.LogStreamDescriptor{}, err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[topicID]
	if !ok {
		return varlogpb.LogStreamDescriptor{}, errors.New("no such topic")
	}

	if !topicDesc.HasLogStream(logStreamID) {
		return varlogpb.LogStreamDescriptor{}, errors.New("no such log stream")
	}

	logStreamDesc, ok := c.vt.logStreams[logStreamID]
	if !ok {
		return varlogpb.LogStreamDescriptor{}, errors.New("no such log stream")
	}

	logStreamDesc = *proto.Clone(&logStreamDesc).(*varlogpb.LogStreamDescriptor)
	head, tail := c.peek(topicID, logStreamID)
	logStreamDesc.Head = head
	logStreamDesc.Tail = tail
	return logStreamDesc, nil
}

func (c *testLog) peek(topicID types.TopicID, logStreamID types.LogStreamID) (head varlogpb.LogEntryMeta, tail varlogpb.LogEntryMeta) {
	head.TopicID = topicID
	head.LogStreamID = logStreamID
	head.GLSN = c.vt.globalLogEntries[topicID][0].GLSN
	head.LLSN = c.vt.localLogEntries[logStreamID][0].LLSN

	tail.TopicID = topicID
	tail.LogStreamID = logStreamID
	lastGIdx := len(c.vt.globalLogEntries[topicID]) - 1
	tail.GLSN = c.vt.globalLogEntries[topicID][lastGIdx].GLSN
	lastLIdx := len(c.vt.localLogEntries[logStreamID]) - 1
	tail.LLSN = c.vt.localLogEntries[logStreamID][lastLIdx].LLSN

	return head, tail
}
