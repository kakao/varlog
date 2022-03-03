package storage

import (
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

const PebbleStorageName = "pebble"

type pebbleStorage struct {
	config

	db                      *pebble.DB
	writeOption             *pebble.WriteOptions
	commitOption            *pebble.WriteOptions
	commitContextOption     *pebble.WriteOptions
	deleteCommittedOption   *pebble.WriteOptions
	deleteUncommittedOption *pebble.WriteOptions

	writeProgress struct {
		mu       sync.RWMutex
		prevLLSN types.LLSN
	}

	commitProgress struct {
		mu       sync.RWMutex
		prevLLSN types.LLSN
		prevGLSN types.GLSN
	}
}

var _ Storage = (*pebbleStorage)(nil)

func newPebbleStorage(cfg *config) (Storage, error) {
	// TODO: make configurable
	// So far, belows is experimental settings.
	pebbleOpts := &pebble.Options{
		ErrorIfExists:               false,
		MemTableSize:                cfg.memTableSizeBytes,
		MemTableStopWritesThreshold: cfg.memTableStopWritesThreshold,
		// quite performance gain, but not durable
		// DisableWAL:                  true,
		L0CompactionThreshold:    2,
		L0StopWritesThreshold:    1000,
		LBaseMaxBytes:            64 << 20,
		Levels:                   make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions: 3,
		MaxOpenFiles:             16384,
	}

	if cfg.debugLog {
		pebbleOpts.EventListener = pebble.MakeLoggingEventListener(cfg.logger.Sugar())
	}
	for i := 0; i < len(pebbleOpts.Levels); i++ {
		l := &pebbleOpts.Levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = pebbleOpts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	pebbleOpts.Levels[6].FilterPolicy = nil
	pebbleOpts.FlushSplitBytes = pebbleOpts.Levels[0].TargetFileSize
	pebbleOpts.EnsureDefaults()

	db, err := pebble.Open(cfg.path, pebbleOpts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ps := &pebbleStorage{
		config:                  *cfg,
		db:                      db,
		writeOption:             &pebble.WriteOptions{Sync: cfg.writeSync},
		commitOption:            &pebble.WriteOptions{Sync: cfg.commitSync},
		deleteCommittedOption:   &pebble.WriteOptions{Sync: cfg.deleteCommittedSync},
		deleteUncommittedOption: &pebble.WriteOptions{Sync: cfg.deleteUncommittedSync},
	}
	return ps, nil
}

func (ps *pebbleStorage) readLastCommitContext(onlyNonEmpty bool) (CommitContext, bool) {
	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitContextKeyPrefix},
		UpperBound: []byte{commitContextKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()

	iter.Last()
	for iter.Valid() {
		cc := decodeCommitContextKey(iter.Key())
		if !onlyNonEmpty {
			return cc, true
		}

		if cc.Empty() {
			iter.Prev()
			continue
		}
		return cc, true
	}
	return InvalidCommitContext, false
}

func (ps *pebbleStorage) readLogEntryBoundary() (first varlogpb.LogEntryMeta, last varlogpb.LogEntryMeta, found bool, err error) {
	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: []byte{commitKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()

	if !iter.First() {
		return varlogpb.InvalidLogEntryMeta(), varlogpb.InvalidLogEntryMeta(), false, nil
	}
	firstGLSN := decodeCommitKey(iter.Key())
	firstLE, err := ps.Read(firstGLSN)
	if err != nil {
		return varlogpb.InvalidLogEntryMeta(), varlogpb.InvalidLogEntryMeta(), true, err
	}

	iter.Last()
	lastGLSN := decodeCommitKey(iter.Key())
	lastLE, err := ps.Read(lastGLSN)
	return firstLE.LogEntryMeta, lastLE.LogEntryMeta, true, err
}

func (ps *pebbleStorage) readUncommittedLogEntryBoundary(lastCommitted varlogpb.LogEntryMeta) (types.LLSN, types.LLSN) {
	dk := encodeDataKey(lastCommitted.LLSN + 1)
	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: dk,
		UpperBound: []byte{dataKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()

	if !iter.First() {
		return types.InvalidLLSN, types.InvalidLLSN
	}
	firstUncommittedLLSN := decodeDataKey(iter.Key())

	iter.Last()
	lastUncommittedLLSN := decodeDataKey(iter.Key())

	return firstUncommittedLLSN, lastUncommittedLLSN
}

func (ps *pebbleStorage) ReadRecoveryInfo() (RecoveryInfo, error) {
	ri := RecoveryInfo{}

	lastNonEmptyCC, foundLastNonEmptyCC := ps.readLastCommitContext(true)
	lastCC, foundLastCC := ps.readLastCommitContext(false)
	firstLE, lastLE, foundLE, err := ps.readLogEntryBoundary()
	if err != nil {
		// corrupt: failed to read LE
		return ri, errors.New("corrupt: mismatch between data and commit")
	}

	ri.LogEntryBoundary.First = firstLE
	ri.LogEntryBoundary.Last = lastLE
	ri.LogEntryBoundary.Found = foundLE

	ri.LastCommitContext.CC = lastCC
	ri.LastCommitContext.Found = foundLastCC

	ri.LastNonEmptyCommitContext.CC = lastNonEmptyCC
	ri.LastNonEmptyCommitContext.Found = foundLastNonEmptyCC

	if foundLE {
		firstUncommittedLLSN, lastUncommittedLLSN := ps.readUncommittedLogEntryBoundary(lastLE)
		ri.UncommittedLogEntryBoundary.First = firstUncommittedLLSN
		ri.UncommittedLogEntryBoundary.Last = lastUncommittedLLSN
	}

	if foundLastNonEmptyCC && (!foundLE || lastNonEmptyCC.CommittedGLSNEnd-1 != lastLE.GLSN) {
		return ri, errors.New("corrupt: mismatch between commit context and log entries")
	}
	if !foundLastNonEmptyCC && foundLE {
		return ri, errors.New("corrupt: mismatch between commit context and log entries")
	}
	return ri, nil
}

//func (ps *pebbleStorage) RestoreLogStreamContext(lsc *logstreamcontext.LogStreamContext) bool {
//	ccIter := ps.db.NewIter(&pebble.IterOptions{
//		LowerBound: []byte{commitContextKeyPrefix},
//		UpperBound: []byte{commitContextKeySentinelPrefix},
//	})
//	defer ccIter.Close()
//
//	// If the storage has no CommitContext, it can't restore past storage status and
//	// LogStreamContext.
//	if !ccIter.Last() {
//		return false
//	}
//
//	cc := decodeCommitContextKey(ccIter.Key())
//	ps.logger.Info("restored commit_context", zap.Any("cc", cc))
//
//	cIter := ps.db.NewIter(&pebble.IterOptions{
//		LowerBound: []byte{commitKeyPrefix},
//		UpperBound: []byte{dataKeyPrefix},
//	})
//	defer cIter.Close()
//
//	// If the GLSN of the last committed log matches with the CommitContext, the storage can be
//	// restored by the CommitContext.
//	if cIter.Last() && decodeCommitKey(cIter.Key()) == cc.CommittedGLSNEnd-1 {
//		// happy path
//		ps.setLogStreamContext(cc.HighWatermark, cIter, lsc)
//		return true
//	}
//
//	// If the storage has no committed logs before the CommitContext, it can't restore past
//	// storage status and LogStreamContext.
//	ck := encodeCommitKey(cc.CommittedGLSNBegin)
//	if !cIter.SeekLT(ck) {
//		// no hint to recover
//		return false
//	}
//
//	// Restore the storage and LogStreamContext by using the last committed logs before the
//	// CommitContext.
//	ps.setLogStreamContext(cc.PrevHighWatermark, cIter, lsc)
//	return true
//}
//
//func (ps *pebbleStorage) setLogStreamContext(globalHWM types.GLSN, cIter *pebble.Iterator, lsc *logstreamcontext.LogStreamContext) {
//	lastGLSN := decodeCommitKey(cIter.Key())
//	lastLLSN := decodeDataKey(cIter.Value())
//	cIter.First()
//	firstGLSN := decodeCommitKey(cIter.Key())
//
//	lsc.Rcc.GlobalHighWatermark = globalHWM
//	lsc.Rcc.UncommittedLLSNBegin = lastLLSN + 1
//	lsc.CommittedLLSNEnd.Llsn = lastLLSN + 1
//	lsc.UncommittedLLSNEnd.Store(lastLLSN + 1)
//	lsc.LocalHighWatermark.Store(lastGLSN)
//	lsc.LocalLowWatermark.Store(firstGLSN)
//}

func (ps *pebbleStorage) RestoreStorage(lastWrittenLLSN types.LLSN, lastCommittedLLSN types.LLSN, lastCommittedGLSN types.GLSN) {
	ps.commitProgress.mu.Lock()
	defer ps.commitProgress.mu.Unlock()
	ps.writeProgress.mu.Lock()
	defer ps.writeProgress.mu.Unlock()
	ps.writeProgress.prevLLSN = lastWrittenLLSN
	ps.commitProgress.prevLLSN = lastCommittedLLSN
	ps.commitProgress.prevGLSN = lastCommittedGLSN
}

func (ps *pebbleStorage) Path() string {
	return ps.path
}

func (ps *pebbleStorage) Name() string {
	return PebbleStorageName
}

func (ps *pebbleStorage) Read(glsn types.GLSN) (varlogpb.LogEntry, error) {
	rkb := newCommitKeyBuffer()
	defer rkb.release()

	ck := encodeCommitKeyInternal(glsn, rkb.ck[:])
	dk, ccloser, err := ps.db.Get(ck)
	if err != nil {
		if err == pebble.ErrNotFound {
			err = verrors.ErrNoEntry
		}
		return varlogpb.InvalidLogEntry(), errors.WithStack(err)
	}

	data, dcloser, err := ps.db.Get(dk)
	if err != nil {
		if err == pebble.ErrNotFound {
			err = verrors.ErrNoEntry
		}
		return varlogpb.InvalidLogEntry(), errors.WithStack(err)
	}

	logEntry := varlogpb.LogEntry{
		LogEntryMeta: varlogpb.LogEntryMeta{
			GLSN: glsn,
			LLSN: decodeDataKey(dk),
		},
	}
	if len(data) > 0 {
		logEntry.Data = make([]byte, len(data))
		copy(logEntry.Data, data)
	}
	if err := multierr.Append(errors.WithStack(ccloser.Close()), errors.WithStack(dcloser.Close())); err != nil {
		return varlogpb.InvalidLogEntry(), err
	}
	return logEntry, nil
}

func (ps *pebbleStorage) ReadGE(glsn types.GLSN) (varlogpb.LogEntry, error) {
	ckb := newCommitKeyBuffer()
	defer ckb.release()

	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeCommitKeyInternal(glsn, ckb.ck[:]),
		UpperBound: []byte{commitKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()

	if !iter.First() {
		return varlogpb.InvalidLogEntry(), errors.WithStack(verrors.ErrNoEntry)
	}

	ck := iter.Key()
	dk := iter.Value()

	data, closer, err := ps.db.Get(dk)
	if err != nil {
		if err == pebble.ErrNotFound {
			err = verrors.ErrNoEntry
		}
		return varlogpb.InvalidLogEntry(), errors.WithStack(err)
	}
	defer func() {
		_ = closer.Close()
	}()

	logEntry := varlogpb.LogEntry{
		LogEntryMeta: varlogpb.LogEntryMeta{
			GLSN: decodeCommitKey(ck),
			LLSN: decodeDataKey(dk),
		},
	}
	if len(data) > 0 {
		logEntry.Data = make([]byte, len(data))
		copy(logEntry.Data, data)
	}
	return logEntry, nil
}

func (ps *pebbleStorage) ReadAt(llsn types.LLSN) (varlogpb.LogEntry, error) {
	// NOTE: Scanning by commit context can be better.
	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: []byte{commitKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()

	ck := make([]byte, commitKeyLength)
	iter.First()
	for iter.Valid() {
		currLLSN := decodeCommitValue(iter.Value())
		if currLLSN > llsn {
			break
		}

		currGLSN := decodeCommitKey(iter.Key())
		if currLLSN == llsn {
			return ps.Read(currGLSN)
		}

		delta := llsn - currLLSN
		glsnGuess := currGLSN + types.GLSN(delta)
		iter.SeekGE(encodeCommitKeyInternal(glsnGuess, ck))
	}
	return varlogpb.InvalidLogEntry(), errors.WithStack(verrors.ErrNoEntry)
}

func (ps *pebbleStorage) Scan(scanOpts ...ScanOption) Scanner {
	cfg, err := newScanConfig(scanOpts)
	if err != nil {
		return &invalidPebbleScanner{err: err}
	}

	scanner := &pebbleScanner{
		scanConfig: cfg,
		db:         ps.db,
		logger:     ps.logger,
	}
	iterOpts := &pebble.IterOptions{}
	if scanner.withGLSN {
		begin, end := scanner.begin.GLSN, scanner.end.GLSN
		lkBuf := newCommitKeyBuffer()
		ukBuf := newCommitKeyBuffer()
		scanner.commitKeyBuffers.lower = lkBuf
		scanner.commitKeyBuffers.upper = ukBuf
		iterOpts.LowerBound = encodeCommitKeyInternal(begin, lkBuf.ck[:])
		iterOpts.UpperBound = encodeCommitKeyInternal(end, ukBuf.ck[:])
	} else {
		begin, end := scanner.begin.LLSN, scanner.end.LLSN
		scanner.dataKeyBuffers.lower = newDataKeyBuffer()
		scanner.dataKeyBuffers.upper = newDataKeyBuffer()
		iterOpts.LowerBound = encodeDataKeyInternal(begin, scanner.dataKeyBuffers.lower.dk)
		iterOpts.UpperBound = encodeDataKeyInternal(end, scanner.dataKeyBuffers.upper.dk)

	}
	scanner.iter = ps.db.NewIter(iterOpts)
	scanner.iter.First()
	return scanner
}

//func (ps *pebbleStorage) Write(llsn types.LLSN, data []byte) error {
//	wb := ps.NewWriteBatch()
//	defer wb.Close()
//	if err := wb.Put(llsn, data); err != nil {
//		return err
//	}
//	return wb.Apply()
//}

func (ps *pebbleStorage) NewWriteBatch() WriteBatch {
	ps.writeProgress.mu.RLock()
	defer ps.writeProgress.mu.RUnlock()
	wb := newPebbleWriteBatch()
	wb.b = ps.db.NewBatch()
	wb.ps = ps
	wb.prevWrittenLLSN = ps.writeProgress.prevLLSN
	return wb
}

func (ps *pebbleStorage) applyWriteBatch(pwb *pebbleWriteBatch) error {
	ps.writeProgress.mu.Lock()
	defer ps.writeProgress.mu.Unlock()
	if ps.writeProgress.prevLLSN != pwb.prevWrittenLLSN {
		return errors.New("storage: inconsistent write batch")
	}
	count := pwb.b.Count()
	if err := ps.db.Apply(pwb.b, ps.writeOption); err != nil {
		return errors.WithStack(err)
	}
	ps.writeProgress.prevLLSN += types.LLSN(count)

	return nil
}

//func (ps *pebbleStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
//	cb, _ := ps.NewCommitBatch(CommitContext{})
//	defer func() {
//		_ = cb.Close()
//	}()
//	if err := cb.Put(llsn, glsn); err != nil {
//		return err
//	}
//	return cb.Apply()
//}

func (ps *pebbleStorage) NewCommitBatch(commitContext CommitContext) (CommitBatch, error) {
	if commitContext.CommittedGLSNBegin > commitContext.CommittedGLSNEnd {
		return nil, errors.New("invalid commit context")
	}

	ps.writeProgress.mu.RLock()
	prevWrittenLLSN := ps.writeProgress.prevLLSN
	ps.writeProgress.mu.RUnlock()

	ps.commitProgress.mu.RLock()
	defer ps.commitProgress.mu.RUnlock()

	if ps.commitProgress.prevGLSN >= commitContext.CommittedGLSNBegin {
		return nil, errors.Errorf("invalid commit context: already committed")
	}

	batch := ps.db.NewBatch()
	var cckBuf [commitContextKeyLength]byte
	cck := encodeCommitContextKeyInternal(commitContext, cckBuf[:])
	if err := batch.Set(cck, nil, ps.commitContextOption); err != nil {
		return nil, multierr.Append(errors.WithStack(err), batch.Close())
	}
	pcb := newPebbleCommitBatch()
	pcb.b = batch
	pcb.ps = ps
	pcb.cc = commitContext

	pcb.snapshot.prevWrittenLLSN = prevWrittenLLSN
	pcb.snapshot.prevCommittedLLSN = ps.commitProgress.prevLLSN
	pcb.snapshot.prevCommittedGLSN = ps.commitProgress.prevGLSN

	pcb.progress.prevCommittedLLSN = ps.commitProgress.prevLLSN
	pcb.progress.prevCommittedGLSN = commitContext.CommittedGLSNBegin - 1 // for convenience

	return pcb, nil
}

func (ps *pebbleStorage) applyCommitBatch(pcb *pebbleCommitBatch) error {
	if pcb.progress.prevCommittedGLSN != pcb.cc.CommittedGLSNEnd-1 {
		return errors.New("not enough commits in commit batch")
	}

	numCommits := pcb.numCommits()
	if numCommits < 0 {
		panic("negative number of commits")
	}

	ps.commitProgress.mu.Lock()
	defer ps.commitProgress.mu.Unlock()

	if ps.commitProgress.prevLLSN != pcb.snapshot.prevCommittedLLSN ||
		ps.commitProgress.prevGLSN != pcb.snapshot.prevCommittedGLSN {
		return errors.New("storage: inconsistent commit batch")
	}

	if err := ps.db.Apply(pcb.b, ps.commitOption); err != nil {
		return errors.WithStack(err)
	}
	ps.commitProgress.prevLLSN = pcb.progress.prevCommittedLLSN
	ps.commitProgress.prevGLSN = pcb.progress.prevCommittedGLSN

	return nil
}

func (ps *pebbleStorage) StoreCommitContext(cc CommitContext) error {
	// TODO (jun): remove commmit context (trim? ttl?)
	cck := encodeCommitContextKey(cc)
	return ps.db.Set(cck, nil, ps.commitContextOption)
}

func (ps *pebbleStorage) ReadFloorCommitContext(ver types.Version) (CommitContext, error) {
	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitContextKeyPrefix},
		UpperBound: []byte{commitContextKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()

	if !iter.Last() {
		return InvalidCommitContext, ErrNotFoundCommitContext
	}

	cc := decodeCommitContextKey(iter.Key())
	if cc.Version <= ver {
		return InvalidCommitContext, ErrNotFoundCommitContext
	}

	for iter.Prev() {
		prev := decodeCommitContextKey(iter.Key())
		if prev.Version <= ver {
			return cc, nil
		}

		cc = prev
	}

	return cc, nil
}

func (ps *pebbleStorage) CommitContextOf(glsn types.GLSN) (CommitContext, error) {
	if glsn.Invalid() {
		return InvalidCommitContext, ErrNotFoundCommitContext
	}
	lowerKey := encodeCommitContextKey(CommitContext{
		HighWatermark: glsn,
	})
	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerKey,
		UpperBound: []byte{commitContextKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()

	if !iter.First() {
		return InvalidCommitContext, ErrNotFoundCommitContext
	}

	if cc := decodeCommitContextKey(iter.Key()); cc.CommittedGLSNBegin <= glsn && glsn < cc.CommittedGLSNEnd {
		return cc, nil
	}
	return InvalidCommitContext, ErrNotFoundCommitContext
}

func (ps *pebbleStorage) NextCommitContextOf(commitContext CommitContext) (CommitContext, error) {
	cck := encodeCommitContextKey(commitContext)
	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: cck,
		UpperBound: []byte{commitContextKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()
	if !iter.First() {
		return InvalidCommitContext, ErrNotFoundCommitContext
	}
	if !decodeCommitContextKey(iter.Key()).Equal(commitContext) {
		return InvalidCommitContext, ErrNotFoundCommitContext
	}
	if !iter.Next() {
		return InvalidCommitContext, ErrNotFoundCommitContext
	}
	return decodeCommitContextKey(iter.Key()), nil
}

func (ps *pebbleStorage) DeleteCommitted(prefixEnd types.GLSN) error {
	if prefixEnd.Invalid() {
		return errors.New("storage: invalid range")
	}

	ps.commitProgress.mu.RLock()
	defer ps.commitProgress.mu.RUnlock()

	// it can't delete uncommitted logs
	if prefixEnd > ps.commitProgress.prevGLSN+1 {
		return errors.New("storage: invalid range")
	}

	cBegin := []byte{commitKeyPrefix}
	cEnd := encodeCommitKey(prefixEnd)

	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: cBegin,
		UpperBound: cEnd,
	})
	defer func() {
		_ = iter.Close()
	}()

	if !iter.Last() {
		// already deleted
		return nil
	}
	lastDataKey := iter.Value()

	// delete committed
	if err := ps.db.DeleteRange(cBegin, cEnd, pebble.NoSync); err != nil {
		return errors.WithStack(err)
	}

	// deleted written
	dBegin := []byte{dataKeyPrefix}
	dEnd := encodeDataKey(decodeDataKey(lastDataKey) + 1)
	return errors.WithStack(ps.db.DeleteRange(dBegin, dEnd, pebble.NoSync))
}

func (ps *pebbleStorage) DeleteUncommitted(suffixBegin types.LLSN) error {
	if suffixBegin.Invalid() {
		return errors.New("storage: invalid range")
	}

	ps.commitProgress.mu.RLock()
	defer ps.commitProgress.mu.RUnlock()
	ps.writeProgress.mu.Lock()
	defer ps.writeProgress.mu.Unlock()

	// no written logs (empty storage)
	/*
		if ps.writeProgress.prevLLSN.Invalid() {
			return nil
		}
	*/

	// no logs to delete
	if suffixBegin > ps.writeProgress.prevLLSN {
		return nil
		//return errors.Errorf("unwritten logs: %d < %d", ps.writeProgress.prevLLSN, suffixBegin)
	}

	// it can't delete committed logs.
	if suffixBegin <= ps.commitProgress.prevLLSN {
		return errors.Errorf("storage: invalid range (suffixBegin %d <= prev committed LLSN %d)", suffixBegin, ps.commitProgress.prevLLSN)
	}

	// it can't delete unwritten logs.
	/*
		if suffixBegin > ps.writeProgress.prevLLSN {
			return fmt.Errorf("storage: invalid range (suffixBegin %d > prev written LLSN %d)", suffixBegin, ps.writeProgress.prevLLSN)
		}
	*/

	begin := encodeDataKey(suffixBegin)
	end := []byte{dataKeySentinelPrefix}
	if err := ps.db.DeleteRange(begin, end, pebble.NoSync); err != nil {
		return errors.WithStack(err)
	}
	ps.writeProgress.prevLLSN = suffixBegin - 1
	return nil
}

func (ps *pebbleStorage) Close() error {
	ps.logger.Info("close")
	flushErr := errors.WithStack(ps.db.Flush())
	closeErr := errors.WithStack(ps.db.Close())
	return multierr.Append(flushErr, closeErr)
}
