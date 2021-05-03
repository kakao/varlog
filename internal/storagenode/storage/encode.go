package storage

import (
	"encoding/binary"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

const (
	dataKeyPrefix         = byte('d')
	dataKeySentinelPrefix = byte('e')
	dataKeyLength         = 9 // prefix(1) + LLSN(8)

	commitKeyPrefix         = byte('c')
	commitKeySentinelPrefix = byte('d')
	commitKeyLength         = 9 // prefix(1) + GLSN(8)
	commitValueLength       = 9 // dataKey(9) + CommitContext(32)

	commitContextKeyPrefix         = byte('x')
	commitContextKeySentinelPrefix = byte('y')
	commitContextKeyLength         = 33 // prefix(1) + CommitContext(32)
)

type dataKey []byte

func (k dataKey) decode() types.LLSN {
	return decodeDataKey(k)
}

func encodeDataKey(llsn types.LLSN) dataKey {
	key := make([]byte, dataKeyLength)
	return encodeDataKeyInternal(llsn, key)
}

func encodeDataKeyInternal(llsn types.LLSN, key []byte) dataKey {
	key[0] = dataKeyPrefix
	binary.BigEndian.PutUint64(key[1:], uint64(llsn))
	return key
}

func decodeDataKey(k dataKey) types.LLSN {
	if k[0] != dataKeyPrefix || len(k) != dataKeyLength {
		panic("storage: invalid key type")
	}
	return types.LLSN(binary.BigEndian.Uint64(k[1:]))
}

type commitKey []byte

func (k commitKey) decode() types.GLSN {
	return decodeCommitKey(k)
}

func encodeCommitKey(glsn types.GLSN) commitKey {
	key := make([]byte, commitKeyLength)
	return encodeCommitKeyInternal(glsn, key)
}

func encodeCommitKeyInternal(glsn types.GLSN, key []byte) commitKey {
	key[0] = commitKeyPrefix
	binary.BigEndian.PutUint64(key[1:], uint64(glsn))
	return key
}

func decodeCommitKey(k []byte) types.GLSN {
	if k[0] != commitKeyPrefix || len(k) != commitKeyLength {
		panic("storage: invalid key type")
	}
	return types.GLSN(binary.BigEndian.Uint64(k[1:]))
}

type commitValue []byte

func (v commitValue) decode() types.LLSN {
	return decodeCommitValue(v)
}

func encodeCommitValue(llsn types.LLSN) commitValue {
	dk := encodeDataKey(llsn)
	return commitValue(dk)
}

func decodeCommitValue(cv commitValue) types.LLSN {
	return decodeDataKey(dataKey(cv))
}

type commitContextKey []byte

func (k commitContextKey) decode() CommitContext {
	return decodeCommitContextKey(k)
}

func encodeCommitContextKey(cc CommitContext) commitContextKey {
	key := make([]byte, commitContextKeyLength)
	key[0] = commitContextKeyPrefix

	offset := 1
	sz := types.GLSNLen
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.PrevHighWatermark))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.HighWatermark))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedGLSNBegin))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedGLSNEnd))

	return key
}

func decodeCommitContextKey(k commitContextKey) (cc CommitContext) {
	if k[0] != commitContextKeyPrefix || len(k) != commitContextKeyLength {
		panic("invalid key type")
	}
	sz := types.GLSNLen
	offset := 1
	cc.PrevHighWatermark = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.HighWatermark = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.CommittedGLSNBegin = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.CommittedGLSNEnd = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	return cc
}
