package metadata_repository

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/proto/mrpb"
)

type StateMachineLogSnapshotGetter interface {
	GetStateMachineLogSnapshot() *mrpb.StateMachineLogSnapshot
}

type StateMachineLog interface {
	OpenForWrite() error

	OpenForRead() error

	Append(*mrpb.StateMachineLogEntry) error

	Read() (interface{}, error)

	Close()
}

var (
	segmentSizeBytes = int64(64 * 1024 * 1024)
	lenFieldLength   = 8
	maxRecordSize    = int(segmentSizeBytes) - lenFieldLength
	crcTable         = crc32.MakeTable(crc32.Castagnoli)
)

type stateMachineLog struct {
	lg      *zap.Logger
	dir     string
	seq     uint64
	file    *os.File
	filePos int

	encoder    StateMachineLogEncoder
	decoder    StateMachineLogDecoder
	snapGetter StateMachineLogSnapshotGetter
}

func newStateMachineLog(lg *zap.Logger, dirpath string, snapGetter StateMachineLogSnapshotGetter) *stateMachineLog {
	cleanupTemp(dirpath)

	return &stateMachineLog{
		lg:         lg,
		dir:        dirpath,
		snapGetter: snapGetter,
	}
}

func (sml *stateMachineLog) createNewSegment() error {
	var err error

	sml.Close()

	path := filepath.Join(sml.dir, fmt.Sprintf("%020d.sml", sml.seq))
	tpath := filepath.Join(sml.dir, fmt.Sprintf("%020d.tmp", sml.seq))
	sml.seq++

	sml.file, err = os.OpenFile(tpath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		sml.lg.Warn(
			"failed to create SML file",
			zap.String("path", tpath),
			zap.Error(err),
		)
		return err
	}

	defer func() {
		if err != nil {
			sml.Close()
		}
	}()

	if _, err = sml.file.Seek(0, io.SeekEnd); err != nil {
		sml.lg.Warn(
			"failed to seek an initial SML file",
			zap.String("path", tpath),
			zap.Error(err),
		)
		return err
	}

	if err = fileutil.Preallocate(sml.file, segmentSizeBytes, true); err != nil {
		sml.lg.Warn(
			"failed to preallocate an initial SML file",
			zap.String("path", tpath),
			zap.Int64("segment-bytes", segmentSizeBytes),
			zap.Error(err),
		)
		return err
	}

	if _, err = sml.file.Seek(0, io.SeekStart); err != nil {
		sml.lg.Warn(
			"failed to seek an initial SML file",
			zap.String("path", tpath),
			zap.Error(err),
		)
		return err
	}

	sml.encoder, err = newStateMachineLogEncoder(sml.file)
	if err != nil {
		return err
	}

	snap := sml.snapGetter.GetStateMachineLogSnapshot()
	err = sml.saveSnapshot(snap)
	if err != nil {
		sml.lg.Warn(
			"failed to save snapshot to SML",
			zap.String("path", tpath),
			zap.Error(err),
		)
		return err
	}

	if err := os.Rename(tpath, path); err != nil {
		sml.lg.Warn(
			"failed to rename the temporary SML directory",
			zap.String("tmp-path", tpath),
			zap.String("path", path),
			zap.Error(err),
		)
		return err
	}

	if err = sml.fsyncDir(); err != nil {
		return err
	}

	return nil
}

func (sml *stateMachineLog) OpenForWrite() error {
	var err error

	if fileutil.Exist(sml.dir) {
		if exist(sml.dir) {
			sml.seq = tailSeq(sml.dir) + 1
		}
	} else {
		if err := fileutil.CreateDirAll(sml.dir); err != nil {
			sml.lg.Warn(
				"failed to create a temporary SML directory",
				zap.String("dir-path", sml.dir),
				zap.Error(err),
			)
			return err
		}
	}

	err = sml.createNewSegment()
	if err != nil {
		return err
	}

	return nil
}

func (sml *stateMachineLog) OpenForRead() error {
	var err error

	if !exist(sml.dir) {
		return io.EOF
	}

	p := filepath.Join(sml.dir, tail(sml.dir))
	sml.file, err = os.OpenFile(p, os.O_RDONLY, 0644)
	if err != nil {
		sml.lg.Warn(
			"failed to open SML file",
			zap.String("path", p),
			zap.Error(err),
		)
		return err
	}

	sml.decoder = newStateMachineLogDecoder(sml.file)

	return nil
}

func (sml *stateMachineLog) Append(entry *mrpb.StateMachineLogEntry) error {
	data, err := entry.Marshal()
	if err != nil {
		return err
	}
	rec := &mrpb.StateMachineLogRecord{
		Type: mrpb.StateMechineLogRecordType_entry,
		Data: data,
	}

	return sml.appendRecord(rec)
}

func (sml *stateMachineLog) saveSnapshot(snap *mrpb.StateMachineLogSnapshot) error {
	data, err := snap.Marshal()
	if err != nil {
		return err
	}
	rec := &mrpb.StateMachineLogRecord{
		Type: mrpb.StateMechineLogRecordType_snapshot,
		Data: data,
	}

	return sml.appendRecord(rec)
}

func (sml *stateMachineLog) appendRecord(rec *mrpb.StateMachineLogRecord) error {
	sml.cut(rec)

	n, err := sml.encoder.Encode(rec)
	if err != nil {
		return err
	}

	err = fileutil.Fdatasync(sml.file)
	if err != nil {
		return err
	}

	sml.filePos += n

	return nil
}

func (sml *stateMachineLog) cut(rec *mrpb.StateMachineLogRecord) error {
	if sml.filePos+lenFieldLength+rec.ProtoSize() > int(segmentSizeBytes) {
		return sml.createNewSegment()
	}

	return nil
}

func (sml *stateMachineLog) Read() (interface{}, error) {
	var rec mrpb.StateMachineLogRecord
	if err := sml.decoder.Decode(&rec); err != nil {
		return nil, err
	}

	switch rec.Type {
	case mrpb.StateMechineLogRecordType_snapshot:
		snap := &mrpb.StateMachineLogSnapshot{}
		if err := snap.Unmarshal(rec.Data); err != nil {
			return nil, err
		}

		return snap, nil
	case mrpb.StateMechineLogRecordType_entry:
		entry := &mrpb.StateMachineLogEntry{}
		if err := entry.Unmarshal(rec.Data); err != nil {
			return nil, err
		}

		return entry, nil
	default:
		return nil, fmt.Errorf("unknown record")
	}
}

func (sml *stateMachineLog) Close() {
	if sml.file == nil {
		return
	}

	sml.file.Close()
	sml.file = nil
}

func (sml *stateMachineLog) fsyncDir() error {
	pdir, err := fileutil.OpenDir(filepath.Dir(sml.dir))
	if err != nil {
		sml.lg.Warn(
			"failed to open the parent data directory",
			zap.String("parent-dir-path", filepath.Dir(sml.dir)),
			zap.String("dir-path", sml.dir),
			zap.Error(err),
		)
		return err
	}
	if err = fileutil.Fsync(pdir); err != nil {
		sml.lg.Warn(
			"failed to fsync the parent data directory file",
			zap.String("parent-dir-path", filepath.Dir(sml.dir)),
			zap.String("dir-path", sml.dir),
			zap.Error(err),
		)
		return err
	}

	if err = pdir.Close(); err != nil {
		sml.lg.Warn(
			"failed to close the parent data directory file",
			zap.String("parent-dir-path", filepath.Dir(sml.dir)),
			zap.String("dir-path", sml.dir),
			zap.Error(err),
		)
		return err
	}

	return nil
}

func exist(dir string) bool {
	names, err := fileutil.ReadDir(dir, fileutil.WithExt(".sml"))
	if err != nil {
		return false
	}
	return len(names) != 0
}

func cleanupTemp(dir string) error {
	names, err := fileutil.ReadDir(dir, fileutil.WithExt(".tmp"))
	if err != nil {
		return err
	}

	for _, name := range names {
		if err = os.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
	}

	return nil
}

func tail(dir string) string {
	names, err := fileutil.ReadDir(dir, fileutil.WithExt(".sml"))
	if err != nil {
		return ""
	}

	if len(names) == 0 {
		return ""
	}

	return names[len(names)-1]
}

func tailSeq(dir string) uint64 {
	t := tail(dir)
	if len(t) == 0 {
		return 0
	}

	ext := filepath.Ext(t)
	t = t[0 : len(t)-len(ext)]
	seq, _ := strconv.Atoi(t)

	return uint64(seq)
}
