package storage

import (
	"encoding/binary"
	"unsafe"

	"github.com/kakao/varlog/pkg/types"
)

const (
	dataKeyPrefix         = byte('d')
	dataKeySentinelPrefix = byte('e')
	dataKeyLength         = 9 // prefix(1) + LLSN(8)

	commitKeyPrefix         = byte('c')
	commitKeySentinelPrefix = byte('d')
	commitKeyLength         = 9 // prefix(1) + GLSN(8)

	// Deprecated: Log stream replica will not store a sequence of commit contexts.
	commitContextKeyPrefix         = byte('x')
	commitContextKeySentinelPrefix = byte('y')
	commitContextKeyLength         = 41 // prefix(1) + CommitContext(40)

	commitContextKeyMarker = byte('b')
	commitContextLength    = 40
)

var commitContextKey = []byte{commitContextKeyMarker}

func encodeDataKeyInternal(llsn types.LLSN, key []byte) []byte {
	key[0] = dataKeyPrefix
	binary.BigEndian.PutUint64(key[1:], uint64(llsn))
	return key
}

func decodeDataKey(k []byte) types.LLSN {
	if k[0] != dataKeyPrefix || len(k) != dataKeyLength {
		panic("storage: invalid key type")
	}
	return types.LLSN(binary.BigEndian.Uint64(k[1:]))
}

func encodeCommitKeyInternal(glsn types.GLSN, key []byte) []byte {
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

func encodeCommitContextKeyInternal(cc CommitContext, key []byte) []byte {
	key[0] = commitContextKeyPrefix

	sz := types.GLSNLen
	offset := 1
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.HighWatermark))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedGLSNBegin))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedGLSNEnd))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedLLSNBegin))

	offset += sz
	sz = types.VersionLen
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.Version))

	return key
}

func decodeCommitContextKey(k []byte) (cc CommitContext) {
	if k[0] != commitContextKeyPrefix || len(k) != commitContextKeyLength {
		panic("storage: invalid key type")
	}
	sz := types.GLSNLen
	offset := 1
	cc.HighWatermark = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.CommittedGLSNBegin = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.CommittedGLSNEnd = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.CommittedLLSNBegin = types.LLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	sz = types.VersionLen
	cc.Version = types.Version(binary.BigEndian.Uint64(k[offset : offset+sz]))

	return cc
}

// encodeCommitContext serializes commit context into byte slice.
func encodeCommitContext(cc CommitContext, key []byte) []byte {
	sz := types.GLSNLen
	offset := 0
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.HighWatermark))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedGLSNBegin))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedGLSNEnd))

	offset += sz
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.CommittedLLSNBegin))

	offset += sz
	sz = types.VersionLen
	binary.BigEndian.PutUint64(key[offset:offset+sz], uint64(cc.Version))

	return key
}

// decodeCommitContext deserializes a commit context from a byte slice.
func decodeCommitContext(k []byte) (cc CommitContext) {
	if len(k) != commitContextLength {
		panic("storage: invalid key type")
	}
	sz := types.GLSNLen
	offset := 0
	cc.HighWatermark = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.CommittedGLSNBegin = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.CommittedGLSNEnd = types.GLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	cc.CommittedLLSNBegin = types.LLSN(binary.BigEndian.Uint64(k[offset : offset+sz]))

	offset += sz
	sz = types.VersionLen
	cc.Version = types.Version(binary.BigEndian.Uint64(k[offset : offset+sz]))

	return cc
}

// encodeCommitContextUnsafe is similar to encodeCommitContext except that it
// shares same memory with the argument cc.
//
// Experimental: It casts struct CommitContext to byte slice; hence its byte
// representation differs across architectures. A user should call
// decodeCommitContextUnsafe to decode bytes encoded by this function.
func encodeCommitContextUnsafe(cc *CommitContext) []byte {
	return (*(*[commitContextLength]byte)(unsafe.Pointer(cc)))[:]
}

// decodeCommitContextUnsafe is similar to decodeCommitContext.
//
// Experimental: A user has to use this function to decode a byte slice encoded
// by encodeCommitContextUnsafe.
func decodeCommitContextUnsafe(buf []byte) (cc CommitContext) {
	cc = *(*CommitContext)(unsafe.Pointer(&buf[0]))
	return cc
}
