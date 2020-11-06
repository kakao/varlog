package storagenode

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
)

const PebbleStorageName = "pebble"

const (
	commitKeyPrefix     = byte('c')
	dataKeyPrefix       = byte('d')
	dataKeySentryPrefix = byte('e')
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
		GLSN: makeGLSN(ck),
		LLSN: makeLLSN(dk),
		Data: retdata,
	}
	scanner.iter.Next()
	return ScanResult{LogEntry: logEntry}
}

func (scanner *pebbleScanner) Close() error {
	return scanner.iter.Close()
}

type pebbleWriteBatch struct {
	b               *pebble.Batch
	ps              *pebbleStorage
	prevWrittenLLSN types.LLSN
}

func (pwb *pebbleWriteBatch) Put(llsn types.LLSN, data []byte) error {
	prevWrittenLLSN := pwb.prevWrittenLLSN + types.LLSN(pwb.b.Count())
	if !prevWrittenLLSN.Invalid() && prevWrittenLLSN+1 != llsn {
		return fmt.Errorf("storage: incorrect LLSN, prev_llsn=%v curr_llsn=%v", prevWrittenLLSN, llsn)
	}

	k := makeDataKey(llsn)
	if err := pwb.b.Set(k, data, pwb.ps.writeOption); err != nil {
		return err
	}
	return nil
}

func (pwb *pebbleWriteBatch) Apply() error {
	return pwb.ps.writeBatch(pwb)
}

func (pwb *pebbleWriteBatch) Close() error {
	return pwb.b.Close()
}

type pebbleCommitBatch struct {
	b                 *pebble.Batch
	ps                *pebbleStorage
	prevCommittedLLSN types.LLSN // fixed value
	prevCommittedGLSN types.GLSN // increment value
}

func (pcb *pebbleCommitBatch) Put(llsn types.LLSN, glsn types.GLSN) error {
	prevCommittedLLSN := pcb.prevCommittedLLSN + types.LLSN(pcb.b.Count())
	if !prevCommittedLLSN.Invalid() && prevCommittedLLSN+1 != llsn {
		return fmt.Errorf("storage: incorrect LLSN, prev_llsn=%v curr_llsn=%v", prevCommittedLLSN, llsn)
	}
	if pcb.prevCommittedGLSN >= glsn {
		return fmt.Errorf("storage: incorrect GLSN, prev_glsn=%v curr_glsn=%v", pcb.prevCommittedGLSN, glsn)
	}

	db := pcb.ps.db
	dk := makeDataKey(llsn)
	_, closer, err := db.Get(dk)
	if err != nil {
		if err == pebble.ErrNotFound {
			return verrors.ErrNoEntry
		}
		return err
	}
	if err := closer.Close(); err != nil {
		return err
	}

	ck := makeCommitKey(glsn)
	if err := pcb.b.Set(ck, dk, pcb.ps.commitOption); err != nil {
		return err
	}
	pcb.prevCommittedGLSN = glsn
	return nil
}

func (pcb *pebbleCommitBatch) Apply() error {
	return pcb.ps.commitBatch(pcb)
}

func (pcb *pebbleCommitBatch) Close() error {
	return pcb.b.Close()
}

type pebbleStorage struct {
	db                *pebble.DB
	prevWrittenLLSN   types.LLSN
	prevCommittedLLSN types.LLSN
	prevCommittedGLSN types.GLSN

	writeOption  *pebble.WriteOptions
	commitOption *pebble.WriteOptions

	dbpath  string
	logger  *zap.Logger
	options *StorageOptions
}

func newPebbleStorage(opts *StorageOptions) (Storage, error) {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	opts.Logger = opts.Logger.Named("pebblestorage")
	db, err := pebble.Open(opts.Path, &pebble.Options{
		ErrorIfExists: true,
	})
	if err != nil {
		return nil, err
	}
	ps := &pebbleStorage{
		db:      db,
		logger:  opts.Logger,
		dbpath:  opts.Path,
		options: opts,
	}
	ps.writeOption = &pebble.WriteOptions{Sync: !opts.DisableWriteSync}
	ps.commitOption = &pebble.WriteOptions{Sync: !opts.DisableWriteSync}
	// TODO (jun): When restarting the SN, pebbleStorage should recover prevLLSN and prevGLSN.
	return ps, nil
}

func makeDataKey(llsn types.LLSN) []byte {
	key := make([]byte, types.LLSNLen+1)
	key[0] = dataKeyPrefix
	binary.BigEndian.PutUint64(key[1:], uint64(llsn))
	return key
}

func makeCommitKey(glsn types.GLSN) []byte {
	key := make([]byte, types.GLSNLen+1)
	key[0] = commitKeyPrefix
	binary.BigEndian.PutUint64(key[1:], uint64(glsn))
	return key
}

func makeLLSN(dataKey []byte) types.LLSN {
	// TODO: check key prefix
	return types.LLSN(binary.BigEndian.Uint64(dataKey[1:]))
}

func makeGLSN(commitKey []byte) types.GLSN {
	// TODO: check key prefix
	return types.GLSN(binary.BigEndian.Uint64(commitKey[1:]))
}

func (ps *pebbleStorage) Path() string {
	return ps.dbpath
}

func (ps *pebbleStorage) Name() string {
	return PebbleStorageName
}

func (ps *pebbleStorage) Read(glsn types.GLSN) (types.LogEntry, error) {
	ck := makeCommitKey(glsn)
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
			ps.logger.Warn("error while closing datakey reader", zap.Any("datakey", dk), zap.Any("llsn", makeLLSN(dk)), zap.Error(err))
		}
	}()
	retdata := make([]byte, len(data))
	copy(retdata, data)
	return types.LogEntry{
		GLSN: glsn,
		LLSN: makeLLSN(dk),
		Data: retdata,
	}, nil
}

func (ps *pebbleStorage) Scan(begin, end types.GLSN) (Scanner, error) {
	opts := &pebble.IterOptions{
		LowerBound: makeCommitKey(begin),
		UpperBound: makeCommitKey(end),
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
	if !ps.prevWrittenLLSN.Invalid() && ps.prevWrittenLLSN+1 != llsn {
		// panic: it can be changed returning an error, and seal itself.
		ps.logger.Panic("try to write incorrect LLSN", zap.Any("prev_llsn", ps.prevWrittenLLSN), zap.Any("curr_llsn", llsn))
	}

	k := makeDataKey(llsn)
	if err := ps.db.Set(k, data, pebble.Sync); err != nil {
		return err
	}

	ps.logger.Debug("write", zap.Any("llsn", llsn))
	ps.prevWrittenLLSN++
	return nil
}

func (ps *pebbleStorage) NewWriteBatch() WriteBatch {
	prevWrittenLLSN := ps.prevWrittenLLSN
	return &pebbleWriteBatch{
		b:               ps.db.NewBatch(),
		prevWrittenLLSN: prevWrittenLLSN,
	}
}

func (ps *pebbleStorage) WriteBatch(entries []WriteEntry) error {
	batch := ps.NewWriteBatch()
	defer func() {
		if err := batch.Close(); err != nil {
			ps.logger.Warn("error while closing batch", zap.Error(err))
		}
	}()
	for _, entry := range entries {
		if err := batch.Put(entry.LLSN, entry.Data); err != nil {
			return err
		}
	}
	return batch.Apply()
}

func (ps *pebbleStorage) writeBatch(pwb *pebbleWriteBatch) error {
	if ps.prevWrittenLLSN != pwb.prevWrittenLLSN {
		return errors.New("incorrect batch")
	}
	count := pwb.b.Count()
	if err := ps.db.Apply(pwb.b, ps.writeOption); err != nil {
		return err
	}
	ps.prevWrittenLLSN += types.LLSN(count)
	return nil
}

func (ps *pebbleStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	if !ps.prevCommittedLLSN.Invalid() && ps.prevCommittedLLSN+1 != llsn {
		ps.logger.Panic("try to commit incorrect LLSN", zap.Any("prev_llsn", ps.prevCommittedLLSN), zap.Any("curr_llsn", llsn))
	}
	if ps.prevCommittedGLSN >= glsn {
		ps.logger.Panic("try to commit incorrect GLSN", zap.Any("prev_glsn", ps.prevCommittedGLSN), zap.Any("curr_glsn", glsn))
	}

	dk := makeDataKey(llsn)
	_, closer, err := ps.db.Get(dk)
	if err != nil {
		if err == pebble.ErrNotFound {
			return verrors.ErrNoEntry
		}
		return err
	}
	if err := closer.Close(); err != nil {
		return err
	}

	ck := makeCommitKey(glsn)
	if err := ps.db.Set(ck, dk, pebble.Sync); err != nil {
		return err
	}

	ps.logger.Debug("commit", zap.Any("llsn", llsn), zap.Any("glsn", glsn))
	ps.prevCommittedGLSN = glsn
	ps.prevCommittedLLSN++
	return nil
}

func (ps *pebbleStorage) CommitBatch(entries []CommitEntry) error {
	batch := ps.NewCommitBatch()
	defer func() {
		if err := batch.Close(); err != nil {
			ps.logger.Warn("error while closing batch", zap.Error(err))
		}
	}()
	for _, entry := range entries {
		if err := batch.Put(entry.LLSN, entry.GLSN); err != nil {
			return err
		}
	}
	return batch.Apply()
}

func (ps *pebbleStorage) NewCommitBatch() CommitBatch {
	return &pebbleCommitBatch{}
}

func (ps *pebbleStorage) commitBatch(pcb *pebbleCommitBatch) error {
	if ps.prevCommittedLLSN != pcb.prevCommittedLLSN {
		return errors.New("incorrect batch")
	}
	count := pcb.b.Count()
	if err := ps.db.Apply(pcb.b, ps.commitOption); err != nil {
		return err
	}
	ps.prevCommittedLLSN += types.LLSN(count)
	ps.prevCommittedGLSN = pcb.ps.prevCommittedGLSN
	return nil
}

func (ps *pebbleStorage) DeleteCommitted(glsn types.GLSN) error {
	begin := []byte{commitKeyPrefix}
	end := makeCommitKey(glsn + 1)
	return ps.db.DeleteRange(begin, end, pebble.NoSync)
}

func (ps *pebbleStorage) DeleteUncommitted(llsn types.LLSN) error {
	if llsn <= ps.prevCommittedLLSN {
		err := fmt.Errorf("storage: could not delete committed (llsn=%v prev_committed_llsn=%v)", llsn, ps.prevCommittedLLSN)
		ps.logger.Error("error while deleting uncommitted logs", zap.Any("llsn", llsn), zap.Any("prev_committed_llsn", ps.prevCommittedLLSN), zap.Error(err))
		return err
	}
	begin := makeDataKey(llsn)
	end := []byte{dataKeySentryPrefix}
	return ps.db.DeleteRange(begin, end, pebble.NoSync)
}

func (ps *pebbleStorage) Close() error {
	return ps.db.Close()
}
