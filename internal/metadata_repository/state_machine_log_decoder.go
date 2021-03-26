package metadata_repository

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	"go.etcd.io/etcd/pkg/crc"

	"github.com/kakao/varlog/proto/mrpb"
)

type StateMachineLogDecoder interface {
	Decode(*mrpb.StateMachineLogRecord) error
}

type stateMachineLogDecoder struct {
	br *bufio.Reader

	crc hash.Hash32
}

func newStateMachineLogDecoder(r io.Reader) *stateMachineLogDecoder {
	return &stateMachineLogDecoder{
		br:  bufio.NewReader(r),
		crc: crc.New(0, crcTable),
	}
}

func (d *stateMachineLogDecoder) Decode(rec *mrpb.StateMachineLogRecord) error {
	rec.Reset()

	if d.br == nil {
		return io.EOF
	}

	recLen, err := d.readLength()
	if err != nil {
		return err
	}

	if recLen > uint64(maxRecordSize) {
		return fmt.Errorf("record size limit")
	} else if recLen == 0 {
		return io.EOF
	}

	data := make([]byte, recLen)
	n, err := io.ReadFull(d.br, data)
	if err != nil {
		return err
	}

	if n != int(recLen) {
		return fmt.Errorf("rec length mismatch")
	}

	if err := rec.Unmarshal(data[:recLen]); err != nil {
		return err
	}

	d.crc.Write(rec.Data)
	if !rec.Validate(d.crc.Sum32()) {
		return fmt.Errorf("crc mismatch")
	}

	return nil
}

func (d *stateMachineLogDecoder) readLength() (uint64, error) {
	return readUint64(d.br)
}

func readUint64(r io.Reader) (uint64, error) {
	var n uint64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
