package varlogtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

type testLog struct {
	vt     *VarlogTest
	closed bool

	lsaPool struct {
		lsaMap map[int64]*logStreamAppender
		nextID int64
		mu     sync.Mutex

		wg sync.WaitGroup
	}
}

var _ varlog.Log = (*testLog)(nil)

func newTestLog(vt *VarlogTest) *testLog {
	c := &testLog{
		vt: vt,
	}
	c.lsaPool.lsaMap = make(map[int64]*logStreamAppender)
	return c
}

func (c *testLog) lock() error {
	c.vt.cond.L.Lock()
	if c.closed {
		c.vt.cond.L.Unlock()
		return verrors.ErrClosed
	}
	return nil
}

func (c *testLog) unlock() {
	c.vt.cond.L.Unlock()
}

func (c *testLog) Close() error {
	c.vt.cond.L.Lock()
	defer c.vt.cond.L.Unlock()
	c.closed = true
	c.vt.cond.Broadcast()

	c.lsaPool.mu.Lock()
	defer c.lsaPool.mu.Unlock()
	for _, lsa := range c.lsaPool.lsaMap {
		lsa.Close()
	}

	c.lsaPool.wg.Wait()
	return nil
}

func (c *testLog) Append(ctx context.Context, topicID types.TopicID, dataBatch [][]byte, opts ...varlog.AppendOption) (res varlog.AppendResult) {
	if err := c.lock(); err != nil {
		res.Err = err
		return res
	}
	defer c.unlock()

	topicDesc, err := c.vt.topicDescriptor(topicID)
	if err != nil {
		res.Err = err
		return res
	}
	logStreamID := topicDesc.LogStreams[c.vt.rng.Intn(len(topicDesc.LogStreams))]
	return c.appendTo(topicID, logStreamID, dataBatch)
}

func (c *testLog) AppendTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, dataBatch [][]byte, opts ...varlog.AppendOption) (res varlog.AppendResult) {
	if err := c.lock(); err != nil {
		res.Err = err
		return res
	}
	defer c.unlock()

	return c.appendTo(topicID, logStreamID, dataBatch)
}

func (c *testLog) appendTo(topicID types.TopicID, logStreamID types.LogStreamID, dataBatch [][]byte) (res varlog.AppendResult) {
	logStreamDesc, err := c.vt.logStreamDescriptor(topicID, logStreamID)
	if err != nil {
		res.Err = err
		return res
	}

	if logStreamDesc.Status.Sealed() {
		res.Err = fmt.Errorf("could not append: %w", verrors.ErrSealed)
		return res
	}

	if _, ok := c.vt.localLogEntries[logStreamID]; !ok {
		res.Err = errors.New("no such log stream")
		return res
	}

	n := len(c.vt.globalLogEntries[topicID])
	lastGLSN := c.vt.globalLogEntries[topicID][n-1].GLSN
	_, tail := c.vt.peek(topicID, logStreamID)
	lastLLSN := tail.LLSN

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
	c.vt.version++
	c.vt.cond.Broadcast()
	return res
}

func (c *testLog) Read(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, glsn types.GLSN) ([]byte, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	topicDesc, err := c.vt.topicDescriptor(topicID)
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

	_, err := c.vt.topicDescriptor(topicID)
	if err != nil {
		return nil, err
	}

	if c.vt.trimGLSNs[topicID] >= begin {
		return nil, errors.New("trimmed")
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
		onNextFunc(varlogpb.LogEntry{}, io.EOF)
	}()

	return func() {
		wg.Wait()
	}, nil
}

func (c *testLog) SubscribeTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.LLSN, opts ...varlog.SubscribeOption) varlog.Subscriber {
	if begin >= end {
		return newErrSubscriber(errors.New("invalid range: begin should be greater than end"))
	}

	if begin.Invalid() || end.Invalid() {
		return newErrSubscriber(errors.New("invalid range: invalid LLSN"))
	}

	if err := c.lock(); err != nil {
		return newErrSubscriber(verrors.ErrClosed)
	}
	defer c.unlock()

	td, err := c.vt.topicDescriptor(topicID)
	if err != nil {
		return newErrSubscriber(err)
	}

	if !td.HasLogStream(logStreamID) {
		return newErrSubscriber(errors.New("no such log stream"))
	}

	localLogEntries, ok := c.vt.localLogEntries[logStreamID]
	if !ok {
		return newErrSubscriber(errors.New("no such log stream"))
	}

	if len(localLogEntries) > int(begin) && localLogEntries[begin].GLSN <= c.vt.trimGLSNs[topicID] {
		return newErrSubscriber(verrors.ErrTrimmed)
	}

	s := &subscriberImpl{
		quit:   make(chan struct{}),
		end:    end,
		cursor: begin,
	}
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
		return c.closed
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

func (c *testLog) PeekLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (first varlogpb.LogSequenceNumber, last varlogpb.LogSequenceNumber, _ bool, err error) {
	if err = c.lock(); err != nil {
		return first, last, false, err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[tpid]
	if !ok {
		return first, last, false, errors.New("no such topic")
	}

	if !topicDesc.HasLogStream(lsid) {
		return first, last, false, errors.New("no such log stream")
	}

	head, tail := c.vt.peek(tpid, lsid)
	first = varlogpb.LogSequenceNumber{
		LLSN: head.LLSN,
		GLSN: head.GLSN,
	}
	last = varlogpb.LogSequenceNumber{
		LLSN: tail.LLSN,
		GLSN: tail.GLSN,
	}
	return first, last, true, nil
}

func (c *testLog) AppendableLogStreams(tpid types.TopicID) map[types.LogStreamID]struct{} {
	if err := c.lock(); err != nil {
		return nil
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[tpid]
	if !ok {
		return nil
	}

	ret := make(map[types.LogStreamID]struct{}, len(topicDesc.LogStreams))
	for _, lsid := range topicDesc.LogStreams {
		ret[lsid] = struct{}{}
	}
	return ret
}

// NewLogStreamAppender returns a new fake LogStreamAppender for testing. It
// ignores options; the pipeline size is five, and the default callback has no
// operation.
func (c *testLog) NewLogStreamAppender(tpid types.TopicID, lsid types.LogStreamID, _ ...varlog.LogStreamAppenderOption) (varlog.LogStreamAppender, error) {
	const pipelineSize = 5

	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	_, err := c.vt.logStreamDescriptor(tpid, lsid)
	if err != nil {
		return nil, err
	}

	lsa := &logStreamAppender{
		c:               c,
		tpid:            tpid,
		lsid:            lsid,
		pipelineSize:    pipelineSize,
		defaultCallback: func([]varlogpb.LogEntryMeta, error) {},
	}
	lsa.queue.ch = make(chan *queueEntry, pipelineSize)
	lsa.queue.cv = sync.NewCond(&lsa.queue.mu)

	c.lsaPool.mu.Lock()
	defer c.lsaPool.mu.Unlock()
	id := c.lsaPool.nextID
	c.lsaPool.nextID++
	c.lsaPool.lsaMap[id] = lsa

	c.lsaPool.wg.Add(1)
	go func() {
		defer func() {
			c.lsaPool.wg.Done()

			c.lsaPool.mu.Lock()
			defer c.lsaPool.mu.Unlock()
			delete(c.lsaPool.lsaMap, id)
		}()

		for qe := range lsa.queue.ch {
			qe.callback(qe.result.Metadata, qe.result.Err)
			lsa.queue.cv.L.Lock()
			lsa.queue.cv.Broadcast()
			lsa.queue.cv.L.Unlock()
		}
	}()

	return lsa, nil
}

type queueEntry struct {
	callback varlog.BatchCallback
	result   varlog.AppendResult
}

type logStreamAppender struct {
	c               *testLog
	tpid            types.TopicID
	lsid            types.LogStreamID
	pipelineSize    int
	defaultCallback varlog.BatchCallback

	closed struct {
		value bool
		sync.Mutex
	}
	queue struct {
		ch chan *queueEntry
		cv *sync.Cond
		mu sync.Mutex
	}
}

var _ varlog.LogStreamAppender = (*logStreamAppender)(nil)

func (lsa *logStreamAppender) AppendBatch(dataBatch [][]byte, callback varlog.BatchCallback) error {
	lsa.closed.Lock()
	defer lsa.closed.Unlock()

	if lsa.closed.value {
		return varlog.ErrClosed
	}

	lsa.queue.cv.L.Lock()
	defer lsa.queue.cv.L.Unlock()

	for len(lsa.queue.ch) >= lsa.pipelineSize {
		lsa.queue.cv.Wait()
	}

	qe := &queueEntry{
		callback: callback,
	}
	qe.result = lsa.c.AppendTo(context.Background(), lsa.tpid, lsa.lsid, dataBatch)
	if qe.callback == nil {
		qe.callback = lsa.defaultCallback
	}
	lsa.queue.ch <- qe
	return nil
}

func (lsa *logStreamAppender) Close() {
	lsa.closed.Lock()
	defer lsa.closed.Unlock()
	if lsa.closed.value {
		return
	}
	lsa.closed.value = true
	close(lsa.queue.ch)
}

type errSubscriber struct {
	err error
}

func newErrSubscriber(err error) *errSubscriber {
	return &errSubscriber{err: err}
}

func (s errSubscriber) Next() (varlogpb.LogEntry, error) {
	return varlogpb.LogEntry{}, s.err
}

func (s errSubscriber) Close() error {
	return s.err
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
		return varlogpb.LogEntry{}, err
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
