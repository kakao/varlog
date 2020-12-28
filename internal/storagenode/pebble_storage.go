package storagenode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
)

const PebbleStorageName = "pebble"

const (
	commitKeyPrefix              = byte('c')
	dataKeyPrefix                = byte('d')
	dataKeySentryPrefix          = byte('e')
	commitContextKeyPrefix       = byte('t')
	commitContextKeySentryPrefix = byte('u')
)

var _ Storage = (*pebbleStorage)(nil)
var _ Scanner = (*pebbleScanner)(nil)

type pebbleScanner struct {
	iter   *pebble.Iterator
	db     *pebble.DB
	logger *zap.Logger
}

func (scanner *pebbleScanner) Next() ScanResult {
	if !scanner.iter.Valid() {
		return NewInvalidScanResult(ErrEndOfRange)
	}
	ck := scanner.iter.Key()
	dk := scanner.iter.Value()
	data, closer, err := scanner.db.Get(dk)
	if err != nil {
		return NewInvalidScanResult(err)
	}
	retdata := make([]byte, len(data))
	copy(retdata, data)
	defer func() {
		if err := closer.Close(); err != nil {
			scanner.logger.Warn("error while closing scanner", zap.Error(err))
		}
	}()
	logEntry := types.LogEntry{
		GLSN: decodeCommitKey(ck),
		LLSN: decodeDataKey(dk),
		Data: retdata,
	}
	scanner.iter.Next()
	return ScanResult{LogEntry: logEntry}
}

func (scanner *pebbleScanner) Close() error {
	return scanner.iter.Close()
}

type pebbleCommitBatch struct {
	b                 *pebble.Batch
	ps                *pebbleStorage
	prevWrittenLLSN   types.LLSN // snapshot
	prevCommittedLLSN types.LLSN // fixed value
	prevCommittedGLSN types.GLSN // increment value
}

func (pcb *pebbleCommitBatch) Put(llsn types.LLSN, glsn types.GLSN) error {
	if llsn.Invalid() || glsn.Invalid() {
		return errors.New("storage: invalid log position")
	}

	batchSize := pcb.b.Count()
	prevCommittedLLSN := pcb.prevCommittedLLSN + types.LLSN(batchSize)
	if batchSize > 0 && prevCommittedLLSN.Invalid() {
		return errors.New("storage: invalid batch")
	}
	if !prevCommittedLLSN.Invalid() && prevCommittedLLSN+1 != llsn {
		return fmt.Errorf("storage: incorrect LLSN, prev_llsn=%v curr_llsn=%v", prevCommittedLLSN, llsn)
	}
	if pcb.prevCommittedGLSN >= glsn {
		return fmt.Errorf("storage: incorrect GLSN, prev_glsn=%v curr_glsn=%v", pcb.prevCommittedGLSN, glsn)
	}

	if llsn > pcb.prevWrittenLLSN {
		return errors.New("storage: unwritten log")
	}

	ck := encodeCommitKey(glsn)
	dk := encodeDataKey(llsn)
	if err := pcb.b.Set(ck, dk, pcb.ps.commitOption); err != nil {
		return err
	}
	pcb.prevCommittedGLSN = glsn
	return nil
}

func (pcb *pebbleCommitBatch) Apply() error {
	return pcb.ps.applyCommitBatch(pcb)
}

func (pcb *pebbleCommitBatch) Close() error {
	return pcb.b.Close()
}

type pebbleStorage struct {
	db *pebble.DB

	writeProgress struct {
		mu       sync.RWMutex
		prevLLSN types.LLSN
	}

	commitProgress struct {
		mu       sync.RWMutex
		prevLLSN types.LLSN
		prevGLSN types.GLSN
	}

	writeOption             *pebble.WriteOptions
	commitOption            *pebble.WriteOptions
	commitContextOption     *pebble.WriteOptions
	deleteCommittedOption   *pebble.WriteOptions
	deleteUncommittedOption *pebble.WriteOptions

	dbpath  string
	logger  *zap.Logger
	options *StorageOptions
}

func newPebbleStorage(opts *StorageOptions) (Storage, error) {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	opts.Logger = opts.Logger.Named("pebblestorage")

	// TODO: make configurable
	// So far, belows is experimental settings.
	pebbleOpts := &pebble.Options{
		ErrorIfExists: false,

		// quite performance gain, but not durable
		// DisableWAL:                  true,
		// L0CompactionThreshold:       2,
		// L0StopWritesThreshold:       1000,
		// LBaseMaxBytes:               64 << 20,
		// Levels:                      make([]pebble.LevelOptions, 7),
		// MaxConcurrentCompactions:    3,
		//  MemTableSize:                64 << 20,
		// MemTableStopWritesThreshold: 4,
	}
	/*
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
		pebbleOpts.EnsureDefaults()
	*/

	db, err := pebble.Open(opts.Path, pebbleOpts)
	if err != nil {
		return nil, err
	}
	ps := &pebbleStorage{
		db:      db,
		logger:  opts.Logger,
		dbpath:  opts.Path,
		options: opts,

		writeOption:             &pebble.WriteOptions{Sync: opts.EnableWriteFsync},
		commitOption:            &pebble.WriteOptions{Sync: opts.EnableCommitFsync},
		commitContextOption:     &pebble.WriteOptions{Sync: opts.EnableCommitContextFsync},
		deleteCommittedOption:   &pebble.WriteOptions{Sync: opts.EnableDeleteCommittedFsync},
		deleteUncommittedOption: &pebble.WriteOptions{Sync: !opts.DisableDeleteUncommittedFsync},
	}
	return ps, nil
}

func (ps *pebbleStorage) RestoreLogStreamContext(lsc *LogStreamContext) bool {
	ccIter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitContextKeyPrefix},
		UpperBound: []byte{commitContextKeySentryPrefix},
	})
	defer ccIter.Close()

	// If the storage has no CommitContext, it can't restore past storage status and
	// LogStreamContext.
	if !ccIter.Last() {
		return false
	}

	cc := decodeCommitContextKey(ccIter.Key())

	cIter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: []byte{dataKeyPrefix},
	})
	defer cIter.Close()

	// If the GLSN of the last committed log matches with the CommitContext, the storage can be
	// restored by the CommitContext.
	if cIter.Last() && decodeCommitKey(cIter.Key()) == cc.CommittedGLSNEnd-1 {
		// happy path
		ps.setLogStreamContext(cc.HighWatermark, cIter, lsc)
		return true
	}

	// If the storage has no committed logs before the CommitContext, it can't restore past
	// storage status and LogStreamContext.
	if !cIter.SeekLT(encodeCommitKey(cc.CommittedGLSNBegin)) {
		// no hint to recover
		return false
	}

	// Restore the storage and LogStreamContext by using the last committed logs before the
	// CommitContext.
	ps.setLogStreamContext(cc.PrevHighWatermark, cIter, lsc)
	return true
}

func (ps *pebbleStorage) setLogStreamContext(globalHWM types.GLSN, cIter *pebble.Iterator, lsc *LogStreamContext) {
	lastGLSN := decodeCommitKey(cIter.Key())
	lastLLSN := decodeDataKey(cIter.Value())
	cIter.First()
	firstGLSN := decodeCommitKey(cIter.Key())

	lsc.rcc.globalHighwatermark = globalHWM
	lsc.rcc.uncommittedLLSNBegin = lastLLSN + 1
	lsc.committedLLSNEnd.llsn = lastLLSN + 1
	lsc.uncommittedLLSNEnd.Store(lastLLSN + 1)
	lsc.localHighWatermark.Store(lastGLSN)
	lsc.localLowWatermark.Store(firstGLSN)
}

func (ps *pebbleStorage) RestoreStorage(lastLLSN types.LLSN, lastGLSN types.GLSN) {
	ps.commitProgress.mu.Lock()
	defer ps.commitProgress.mu.Unlock()
	ps.writeProgress.mu.Lock()
	defer ps.writeProgress.mu.Unlock()
	ps.writeProgress.prevLLSN = lastLLSN
	ps.commitProgress.prevLLSN = lastLLSN
	ps.commitProgress.prevGLSN = lastGLSN
}

func (ps *pebbleStorage) Path() string {
	return ps.dbpath
}

func (ps *pebbleStorage) Name() string {
	return PebbleStorageName
}

func (ps *pebbleStorage) Read(glsn types.GLSN) (types.LogEntry, error) {
	ck := encodeCommitKey(glsn)
	dk, ccloser, err := ps.db.Get(ck)
	if err != nil {
		if err == pebble.ErrNotFound {
			return types.InvalidLogEntry, verrors.ErrNoEntry
		}
		return types.InvalidLogEntry, err
	}

	data, dcloser, err := ps.db.Get(dk)
	if err != nil {
		if err == pebble.ErrNotFound {
			return types.InvalidLogEntry, verrors.ErrNoEntry
		}
		return types.InvalidLogEntry, err
	}

	defer func() {
		if err := ccloser.Close(); err != nil {
			ps.logger.Warn("error while closing commitkey reader", zap.Any("commitkey", ck), zap.Any("glsn", glsn), zap.Error(err))
		}
		if err := dcloser.Close(); err != nil {
			ps.logger.Warn("error while closing datakey reader", zap.Any("datakey", dk), zap.Any("llsn", decodeDataKey(dk)), zap.Error(err))
		}
	}()
	retdata := make([]byte, len(data))
	copy(retdata, data)
	return types.LogEntry{
		GLSN: glsn,
		LLSN: decodeDataKey(dk),
		Data: retdata,
	}, nil
}

func (ps *pebbleStorage) Scan(begin, end types.GLSN) (Scanner, error) {
	opts := &pebble.IterOptions{
		LowerBound: encodeCommitKey(begin),
		UpperBound: encodeCommitKey(end),
	}
	iter := ps.db.NewIter(opts)
	iter.First()
	return &pebbleScanner{
		iter:   iter,
		logger: ps.logger,
		db:     ps.db,
	}, nil
}

func (ps *pebbleStorage) Write(llsn types.LLSN, data []byte) error {
	wb := ps.NewWriteBatch()
	defer wb.Close()
	if err := wb.Put(llsn, data); err != nil {
		return err
	}
	return wb.Apply()
}

func (ps *pebbleStorage) NewWriteBatch() WriteBatch {
	ps.writeProgress.mu.RLock()
	defer ps.writeProgress.mu.RUnlock()
	wb := &pebbleWriteBatch{}
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
		return err
	}
	ps.writeProgress.prevLLSN += types.LLSN(count)
	return nil
}

func (ps *pebbleStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	cb := ps.NewCommitBatch()
	defer cb.Close()
	if err := cb.Put(llsn, glsn); err != nil {
		return err
	}
	return cb.Apply()
}

func (ps *pebbleStorage) NewCommitBatch() CommitBatch {
	ps.writeProgress.mu.RLock()
	prevWrittenLLSN := ps.writeProgress.prevLLSN
	ps.writeProgress.mu.RUnlock()

	ps.commitProgress.mu.RLock()
	defer ps.commitProgress.mu.RUnlock()
	return &pebbleCommitBatch{
		b:                 ps.db.NewBatch(),
		ps:                ps,
		prevWrittenLLSN:   prevWrittenLLSN,
		prevCommittedLLSN: ps.commitProgress.prevLLSN,
		prevCommittedGLSN: ps.commitProgress.prevGLSN,
	}
}

func (ps *pebbleStorage) applyCommitBatch(pcb *pebbleCommitBatch) error {
	ps.commitProgress.mu.Lock()
	defer ps.commitProgress.mu.Unlock()

	if ps.commitProgress.prevLLSN != pcb.prevCommittedLLSN {
		return errors.New("storage: inconsistent commit batch")
	}
	count := pcb.b.Count()
	if err := ps.db.Apply(pcb.b, ps.commitOption); err != nil {
		return err
	}
	ps.commitProgress.prevLLSN += types.LLSN(count)
	ps.commitProgress.prevGLSN = pcb.prevCommittedGLSN
	return nil
}

func (ps *pebbleStorage) StoreCommitContext(cc CommitContext) error {
	// TODO (jun): remove commmit context (trim? ttl?)
	cck := encodeCommitContextKey(cc)
	return ps.db.Set(cck, nil, ps.commitContextOption)
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
	defer iter.Close()

	if !iter.Last() {
		// already deleted
		return nil
	}
	lastDataKey := iter.Value()

	// delete committed
	if err := ps.db.DeleteRange(cBegin, cEnd, pebble.NoSync); err != nil {
		return err
	}

	// deleted written
	dBegin := []byte{dataKeyPrefix}
	dEnd := encodeDataKey(decodeDataKey(lastDataKey) + 1)
	return ps.db.DeleteRange(dBegin, dEnd, pebble.NoSync)
}

func (ps *pebbleStorage) DeleteUncommitted(suffixBegin types.LLSN) error {
	ps.commitProgress.mu.RLock()
	defer ps.commitProgress.mu.RUnlock()
	ps.writeProgress.mu.Lock()
	defer ps.writeProgress.mu.Unlock()

	// no written logs (empty storage)
	if ps.writeProgress.prevLLSN.Invalid() {
		return nil
	}

	// it can't delete committed logs.
	if suffixBegin <= ps.commitProgress.prevLLSN {
		return fmt.Errorf("storage: invalid range (suffixBegin %d <= prev committed LLSN %d)", suffixBegin, ps.commitProgress.prevLLSN)
	}

	// no logs to delete
	if suffixBegin > ps.writeProgress.prevLLSN {
		return nil
	}

	// it can't delete unwritten logs.
	/*
		if suffixBegin > ps.writeProgress.prevLLSN {
			return fmt.Errorf("storage: invalid range (suffixBegin %d > prev written LLSN %d)", suffixBegin, ps.writeProgress.prevLLSN)
		}
	*/

	begin := encodeDataKey(suffixBegin)
	end := []byte{dataKeySentryPrefix}
	if err := ps.db.DeleteRange(begin, end, pebble.NoSync); err != nil {
		return err
	}
	ps.writeProgress.prevLLSN = suffixBegin - 1
	return nil
}

func (ps *pebbleStorage) Close() error {
	return ps.db.Close()
}

func encodeDataKey(llsn types.LLSN) []byte {
	key := make([]byte, types.LLSNLen+1)
	key[0] = dataKeyPrefix
	binary.BigEndian.PutUint64(key[1:], uint64(llsn))
	return key
}

func decodeDataKey(dataKey []byte) types.LLSN {
	if dataKey[0] != dataKeyPrefix {
		panic("storage: invalid key type")
	}
	return types.LLSN(binary.BigEndian.Uint64(dataKey[1:]))
}

func encodeCommitKey(glsn types.GLSN) []byte {
	key := make([]byte, types.GLSNLen+1)
	key[0] = commitKeyPrefix
	binary.BigEndian.PutUint64(key[1:], uint64(glsn))
	return key
}

func decodeCommitKey(commitKey []byte) types.GLSN {
	if commitKey[0] != commitKeyPrefix {
		panic("storage: invalid key type")
	}
	return types.GLSN(binary.BigEndian.Uint64(commitKey[1:]))
}

func encodeCommitContextKey(cc CommitContext) []byte {
	sz := types.GLSNLen
	key := make([]byte, sz*4+1)

	key[0] = commitContextKeyPrefix
	offset := 1
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.HighWatermark))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.PrevHighWatermark))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedGLSNBegin))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedGLSNEnd))
	return key
}

func decodeCommitContextKey(key []byte) (cc CommitContext) {
	if key[0] != commitContextKeyPrefix {
		panic("storage: invalid key type")
	}

	sz := types.GLSNLen
	offset := 1
	cc.HighWatermark = types.GLSN(binary.BigEndian.Uint64(key[offset : offset+sz]))

	offset += sz
	cc.PrevHighWatermark = types.GLSN(binary.BigEndian.Uint64(key[offset : offset+sz]))

	offset += sz
	cc.CommittedGLSNBegin = types.GLSN(binary.BigEndian.Uint64(key[offset : offset+sz]))

	offset += sz
	cc.CommittedGLSNEnd = types.GLSN(binary.BigEndian.Uint64(key[offset : offset+sz]))
	return cc
}
