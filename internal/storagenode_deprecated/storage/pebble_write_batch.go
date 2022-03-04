package storage

import (
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
)

var pebbleWriteBatchPool = sync.Pool{
	New: func() interface{} {
		return &pebbleWriteBatch{}
	},
}

type pebbleWriteBatch struct {
	b               *pebble.Batch
	ps              *pebbleStorage
	prevWrittenLLSN types.LLSN

	dataKeyArray  [dataKeyLength]byte
	dataKeyBuffer []byte
}

var _ WriteBatch = (*pebbleWriteBatch)(nil)

func newPebbleWriteBatch() *pebbleWriteBatch {
	pwb := pebbleWriteBatchPool.Get().(*pebbleWriteBatch)
	pwb.dataKeyBuffer = pwb.dataKeyArray[:]
	return pwb
}

func (pwb *pebbleWriteBatch) release() {
	pwb.b = nil
	pwb.ps = nil
	pwb.prevWrittenLLSN = types.InvalidLLSN
	pwb.dataKeyBuffer = pwb.dataKeyBuffer[0:0]
	pebbleWriteBatchPool.Put(pwb)
}

func (pwb *pebbleWriteBatch) Put(llsn types.LLSN, data []byte) error {
	if llsn.Invalid() {
		return errors.New("storage: invalid log position")
	}

	numWrite := pwb.b.Count()
	prevWrittenLLSN := pwb.prevWrittenLLSN + types.LLSN(numWrite)
	if prevWrittenLLSN+1 != llsn {
		return errors.Errorf(
			"storage: invalid write batch, incorrect LLSN, expected=%d actual=%d, snapshot=%d, count=%d",
			prevWrittenLLSN+1, llsn, pwb.prevWrittenLLSN, numWrite)
	}

	/*
		if numWrite > 0 && prevWrittenLLSN.Invalid() {
			return errors.New("storage: invalid batch")
		}

		if !prevWrittenLLSN.Invalid() && prevWrittenLLSN+1 != llsn {
			return errors.Errorf("storage: incorrect LLSN, prev_llsn=%v curr_llsn=%v", prevWrittenLLSN, llsn)
		}
	*/

	dk := encodeDataKeyInternal(llsn, pwb.dataKeyBuffer)
	if err := pwb.b.Set(dk, data, pwb.ps.writeOption); err != nil {
		return err
	}
	return nil
}

func (pwb *pebbleWriteBatch) Apply() error {
	return pwb.ps.applyWriteBatch(pwb)
}

func (pwb *pebbleWriteBatch) Size() int {
	return int(pwb.b.Count())
}

func (pwb *pebbleWriteBatch) Close() error {
	err := pwb.b.Close()
	pwb.release()
	return err
}
