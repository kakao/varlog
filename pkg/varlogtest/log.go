package varlogtest

import (
	"context"
	"io"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type testLog struct {
	vt *VarlogTest
}

var _ varlog.Log = (*testLog)(nil)

func (c *testLog) lock() error {
	c.vt.cond.L.Lock()
	if c.vt.varlogClientClosed {
		c.vt.cond.L.Unlock()
		return verrors.ErrClosed
	}
	return nil
}

func (c testLog) unlock() {
	c.vt.cond.L.Unlock()
}

func (c *testLog) Close() error {
	c.vt.cond.L.Lock()
	defer c.vt.cond.L.Unlock()
	c.vt.varlogClientClosed = true
	c.vt.cond.Broadcast()
	return nil
}

func (c *testLog) Append(ctx context.Context, topicID types.TopicID, dataBatch [][]byte, opts ...varlog.AppendOption) (varlog.AppendResult, error) {
	if err := c.lock(); err != nil {
		return varlog.AppendResult{}, err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return varlog.AppendResult{}, errors.New("no such topic")
	}
	if len(topicDesc.LogStreams) == 0 {
		return varlog.AppendResult{}, errors.New("no log stream")
	}

	logStreamID := topicDesc.LogStreams[c.vt.rng.Intn(len(topicDesc.LogStreams))]

	return c.appendTo(topicID, logStreamID, dataBatch)
}

func (c *testLog) AppendTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, dataBatch [][]byte, opts ...varlog.AppendOption) (varlog.AppendResult, error) {
	if err := c.lock(); err != nil {
		return varlog.AppendResult{}, err
	}
	defer c.unlock()

	return c.appendTo(topicID, logStreamID, dataBatch)
}

func (c *testLog) appendTo(topicID types.TopicID, logStreamID types.LogStreamID, dataBatch [][]byte) (varlog.AppendResult, error) {
	topicDesc, err := c.topicDescriptor(topicID)
	if err != nil {
		return varlog.AppendResult{}, err
	}
	if !topicDesc.HasLogStream(logStreamID) {
		return varlog.AppendResult{}, errors.New("no such log stream in the topic")
	}

	if _, ok := c.vt.localLogEntries[logStreamID]; !ok {
		return varlog.AppendResult{}, errors.New("no such log stream")
	}

	n := len(c.vt.globalLogEntries[topicID])
	lastGLSN := c.vt.globalLogEntries[topicID][n-1].GLSN
	_, tail := c.peek(topicID, logStreamID)
	lastLLSN := tail.LLSN

	res := varlog.AppendResult{}
	for _, data := range dataBatch {
		lastGLSN++
		lastLLSN++
		logEntry := &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     topicID,
				LogStreamID: logStreamID,
				GLSN:        lastGLSN,
				LLSN:        lastLLSN,
			},
			Data: make([]byte, len(data)),
		}
		copy(logEntry.Data, data)

		c.vt.globalLogEntries[topicID] = append(c.vt.globalLogEntries[topicID], logEntry)
		c.vt.localLogEntries[logStreamID] = append(c.vt.localLogEntries[logStreamID], logEntry)
		res.Metadata = append(res.Metadata, logEntry.LogEntryMeta)
	}
	c.vt.cond.Broadcast()
	return res, nil
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

func (c *testLog) SubscribeTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.LLSN, opts ...varlog.SubscribeOption) varlog.Subscriber {
	s := &subscriberImpl{
		quit: make(chan struct{}),
	}

	if begin >= end {
		s.err = errors.New("invalid range: begin should be greater than end")
		return s
	}

	if begin.Invalid() || end.Invalid() {
		s.err = errors.New("invalid range: invalid LLSN")
		return s
	}

	if err := c.lock(); err != nil {
		s.err = verrors.ErrClosed
		return s
	}
	defer c.unlock()

	td, err := c.topicDescriptor(topicID)
	if err != nil {
		s.err = err
		return s
	}

	if !td.HasLogStream(logStreamID) {
		s.err = errors.New("no such log stream")
		return s
	}

	_, ok := c.vt.localLogEntries[logStreamID]
	if !ok {
		s.err = errors.New("no such log stream")
		return s
	}

	s.end = end
	s.cursor = begin
	s.quit = make(chan struct{})
	s.contextError = func() error {
		return ctx.Err()
	}
	s.vt.cond = c.vt.cond
	s.vt.logEntries = func() []*varlogpb.LogEntry {
		logEntries, ok := c.vt.localLogEntries[logStreamID]
		if !ok {
			panic("inconsistent status: no such log stream")
		}
		return logEntries
	}
	s.vt.closedClient = func() bool {
		return c.vt.varlogClientClosed
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-ctx.Done():
		case <-s.quit:
		}
		s.vt.cond.L.Lock()
		s.vt.cond.Broadcast()
		s.vt.cond.L.Unlock()
	}()

	return s
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
	tail.TopicID = topicID
	tail.LogStreamID = logStreamID

	if len(c.vt.localLogEntries[logStreamID]) < 2 {
		return
	}

	head.GLSN = c.vt.localLogEntries[logStreamID][1].GLSN
	head.LLSN = c.vt.localLogEntries[logStreamID][1].LLSN
	lastIdx := len(c.vt.localLogEntries[logStreamID]) - 1
	tail.GLSN = c.vt.localLogEntries[logStreamID][lastIdx].GLSN
	tail.LLSN = c.vt.localLogEntries[logStreamID][lastIdx].LLSN
	return head, tail
}

type subscriberImpl struct {
	end    types.LLSN
	cursor types.LLSN

	quit         chan struct{}
	contextError func() error

	mu     sync.Mutex
	once   sync.Once
	err    error
	closed bool

	// localLogEntries of VarlogTest
	vt struct {
		cond         *sync.Cond
		logEntries   func() []*varlogpb.LogEntry
		closedClient func() bool
	}

	wg sync.WaitGroup
}

func (s *subscriberImpl) Next() (varlogpb.LogEntry, error) {
	logEntry, err := s.next()
	if err != nil {
		s.setErr(err)
		return varlogpb.InvalidLogEntry(), err
	}
	if s.cursor == s.end {
		s.setErr(io.EOF)
	}
	return logEntry, nil
}

func (s *subscriberImpl) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true
	close(s.quit)
	s.wg.Wait()

	if s.err == io.EOF {
		return nil
	}
	return s.err
}

func (s *subscriberImpl) next() (logEntry varlogpb.LogEntry, err error) {
	s.vt.cond.L.Lock()
	defer s.vt.cond.L.Unlock()

	for !s.available() && s.contextError() == nil && !s.isClosed() && s.getErr() == nil && !s.vt.closedClient() {
		s.vt.cond.Wait()
	}

	if s.vt.closedClient() {
		return logEntry, errors.New("client closed")
	}

	if err := s.contextError(); err != nil {
		s.setErr(err)
		return logEntry, err
	}

	if s.isClosed() {
		return logEntry, errors.New("closed")
	}

	if err := s.getErr(); err != nil {
		return logEntry, err
	}

	logEntries := s.vt.logEntries()
	logEntry = varlogpb.LogEntry{
		LogEntryMeta: varlogpb.LogEntryMeta{
			TopicID:     logEntries[s.cursor].TopicID,
			LogStreamID: logEntries[s.cursor].LogStreamID,
			GLSN:        logEntries[s.cursor].GLSN,
			LLSN:        s.cursor,
		},
		Data: make([]byte, len(logEntries[s.cursor].Data)),
	}
	copy(logEntry.Data, logEntries[s.cursor].Data)
	s.cursor++
	return logEntry, nil
}

func (s *subscriberImpl) available() bool {
	logEntries := s.vt.logEntries()
	lastIdx := len(logEntries) - 1
	return logEntries[lastIdx].LLSN >= s.cursor
}

func (s *subscriberImpl) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *subscriberImpl) getErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *subscriberImpl) setErr(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.err = err
	})
}
