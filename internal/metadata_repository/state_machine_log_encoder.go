package metadata_repository

import (
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"

	"go.etcd.io/etcd/pkg/crc"

	"github.com/kakao/varlog/proto/mrpb"
)

type StateMachineLogEncoder interface {
	Encode(*mrpb.StateMachineLogRecord) (int, error)

	SumCRC() uint32
}

type stateMachineLogEncoder struct {
	w io.Writer

	crc hash.Hash32
	buf []byte
}

func newStateMachineLogEncoder(f *os.File, prevCrc uint32) (StateMachineLogEncoder, error) {
	_, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	return &stateMachineLogEncoder{
		w:   f,
		crc: crc.New(prevCrc, crcTable),
		buf: make([]byte, 1024*1024),
	}, nil
}

func (e *stateMachineLogEncoder) Encode(rec *mrpb.StateMachineLogRecord) (int, error) {
	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()
	var (
		data []byte
		err  error
	)

	lenRec := rec.ProtoSize()
	if lenRec > maxRecordSize {
		return -1, fmt.Errorf("exceed record size limit")
	}

	if lenRec+lenFieldLength > len(e.buf) {
		data = make([]byte, lenRec+lenFieldLength)
	} else {
		data = e.buf
	}

	_, err = rec.MarshalTo(data[lenFieldLength:])
	if err != nil {
		return -1, err
	}

	// write length
	binary.LittleEndian.PutUint64(data, uint64(lenRec))

	// write
	if _, err = e.w.Write(data[:lenFieldLength+lenRec]); err != nil {
		return -1, err
	}

	return lenFieldLength + lenRec, nil
}

func (e *stateMachineLogEncoder) SumCRC() uint32 {
	return e.crc.Sum32()
}
