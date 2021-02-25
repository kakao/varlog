package storagenode

import (
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
)

type pebbleWriteBatch struct {
	b               *pebble.Batch
	ps              *pebbleStorage
	prevWrittenLLSN types.LLSN
}

var _ WriteBatch = (*pebbleWriteBatch)(nil)

func (pwb *pebbleWriteBatch) Put(llsn types.LLSN, data []byte) error {
	if llsn.Invalid() {
		return errors.New("storage: invalid log position")
	}

	batchSize := pwb.b.Count()
	prevWrittenLLSN := pwb.prevWrittenLLSN + types.LLSN(batchSize)

	if batchSize > 0 && prevWrittenLLSN.Invalid() {
		return errors.New("storage: invalid batch")
	}

	if !prevWrittenLLSN.Invalid() && prevWrittenLLSN+1 != llsn {
		return errors.Errorf("storage: incorrect LLSN, prev_llsn=%v curr_llsn=%v", prevWrittenLLSN, llsn)
	}

	dk := encodeDataKey(llsn)
	return errors.WithStack(pwb.b.Set(dk, data, pwb.ps.writeOption))
}

func (pwb *pebbleWriteBatch) Apply() error {
	return pwb.ps.applyWriteBatch(pwb)
}

func (pwb *pebbleWriteBatch) Close() error {
	return pwb.b.Close()
}
