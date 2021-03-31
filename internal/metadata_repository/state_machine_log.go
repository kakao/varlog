package metadata_repository

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.uber.org/zap"

	"github.com/kakao/varlog/proto/mrpb"
)

type StateMachineLog interface {
	OpenForWrite(uint64) error

	Append(*mrpb.StateMachineLogEntry) error

	ReadFrom(uint64) ([]*mrpb.StateMachineLogEntry, error)

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
	enti    uint64
	file    *os.File
	filePos int

	encoder StateMachineLogEncoder
	decoder StateMachineLogDecoder
}

func newStateMachineLog(lg *zap.Logger, dirpath string) *stateMachineLog {
	return &stateMachineLog{
		lg:  lg,
		dir: dirpath,
	}
}

func (sml *stateMachineLog) createNewSegment(appliedIndex uint64, prevCrc uint32) error {
	var err error

	if sml.file != nil {
		off, serr := sml.file.Seek(0, io.SeekCurrent)
		if serr != nil {
			return serr
		}

		if err := sml.file.Truncate(off); err != nil {
			return err
		}

		if err = fileutil.Fdatasync(sml.file); err != nil {
			return err
		}
		sml.Close()
	}

	path := filepath.Join(sml.dir, fmt.Sprintf("%020d.sml", appliedIndex))
	tpath := filepath.Join(sml.dir, fmt.Sprintf("%020d.tmp", appliedIndex))

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

	sml.encoder, err = newStateMachineLogEncoder(sml.file, prevCrc)
	if err != nil {
		return err
	}

	if err := sml.saveCrc(prevCrc); err != nil {
		sml.lg.Warn(
			"failed to save crc",
			zap.String("tmp-path", tpath),
			zap.String("path", path),
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

func (sml *stateMachineLog) OpenForWrite(appliedIndex uint64) error {
	var err error

	if fileutil.Exist(sml.dir) {
		if err = cleanupStateMachineLog(sml.dir); err != nil {
			sml.lg.Warn(
				"failed to cleanup SML directory",
				zap.String("dir-path", sml.dir),
				zap.Error(err),
			)
			return err
		}
	}

	if err := fileutil.CreateDirAll(sml.dir); err != nil {
		sml.lg.Warn(
			"failed to create a temporary SML directory",
			zap.String("dir-path", sml.dir),
			zap.Error(err),
		)
		return err
	}

	err = sml.createNewSegment(appliedIndex, 0)
	if err != nil {
		return err
	}

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

	if err = sml.appendRecord(rec); err != nil {
		return err
	}

	sml.enti = entry.AppliedIndex

	if sml.filePos < int(segmentSizeBytes) {
		return nil
	}

	return sml.cut()
}

func (sml *stateMachineLog) saveCrc(prevCrc uint32) error {
	return sml.appendRecord(&mrpb.StateMachineLogRecord{
		Type: mrpb.StateMechineLogRecordType_crc,
		Crc:  prevCrc,
	})
}

func (sml *stateMachineLog) appendRecord(rec *mrpb.StateMachineLogRecord) error {
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

func (sml *stateMachineLog) cut() error {
	prevCrc := sml.encoder.SumCRC()
	return sml.createNewSegment(sml.enti+1, prevCrc)
}

func (sml *stateMachineLog) openSegmentForRead(segment string, prevCrc uint32) error {
	var err error

	p := filepath.Join(sml.dir, segment)
	sml.file, err = os.OpenFile(p, os.O_RDONLY, 0644)
	if err != nil {
		sml.lg.Warn(
			"failed to open SML file",
			zap.String("path", p),
			zap.Error(err),
		)
		return err
	}

	sml.decoder = newStateMachineLogDecoder(sml.file, prevCrc)

	return nil
}

func (sml *stateMachineLog) readSegmentAll(appliedIndex uint64, head bool) ([]*mrpb.StateMachineLogEntry, uint32, error) {
	var err error
	var entries []*mrpb.StateMachineLogEntry
	var rec mrpb.StateMachineLogRecord

	for err == nil {
		err = sml.decoder.Decode(&rec)
		if err != nil {
			continue
		}

		switch rec.Type {
		case mrpb.StateMechineLogRecordType_entry:
			entry := &mrpb.StateMachineLogEntry{}
			if err = entry.Unmarshal(rec.Data); err == nil {
				if entry.AppliedIndex >= appliedIndex {
					entries = append(entries, entry)
				}
			}
		case mrpb.StateMechineLogRecordType_crc:
			if head {
				sml.decoder.UpdateCRC(rec.Crc)
			}

			if sml.decoder.SumCRC() != rec.Crc {
				err = fmt.Errorf("crc mismatch. file:%v, head:%v, cur:%v, readn:%v",
					sml.file.Name(), head, sml.decoder.SumCRC(), rec.Crc)
			}

		default:
			err = fmt.Errorf("uknown record")
		}
	}

	if err != io.EOF {
		return nil, 0, err
	}

	return entries, sml.decoder.SumCRC(), nil
}

func (sml *stateMachineLog) ReadFrom(appliedIndex uint64) ([]*mrpb.StateMachineLogEntry, error) {
	segments, err := getStateMachineLogSegments(sml.dir)
	if err != nil {
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			return nil, io.EOF
		}
		return nil, err
	}

	if len(segments) == 0 {
		return nil, io.EOF
	}

	headi, err := selectStateMachineLogSegment(segments, appliedIndex)
	if err != nil {
		return nil, err
	}

	if headi < 0 {
		return nil, io.EOF
	}

	var prevCrc uint32
	var ret []*mrpb.StateMachineLogEntry

	for i := headi; i < len(segments); i++ {
		if err = sml.openSegmentForRead(segments[i], prevCrc); err != nil {
			return nil, err
		}

		var entries []*mrpb.StateMachineLogEntry

		entries, prevCrc, err = sml.readSegmentAll(appliedIndex, i == headi)
		sml.Close()
		if err != nil {
			return nil, err
		}

		ret = append(ret, entries...)
	}

	return ret, err
}

func (sml *stateMachineLog) Close() {
	if sml.file == nil {
		return
	}

	sml.file.Close()
	sml.file = nil
	sml.filePos = 0
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

func getStateMachineLogSegments(dir string) ([]string, error) {
	return fileutil.ReadDir(dir, fileutil.WithExt(".sml"))
}

func existStateMachineLog(dir string) bool {
	names, err := getStateMachineLogSegments(dir)
	if err != nil {
		return false
	}
	return len(names) != 0
}

func cleanupStateMachineLog(dir string) error {
	cleanupDirName := fmt.Sprintf("%s.cleanup.%v", dir, time.Now().Format("20060102.150405.999999"))
	if err := os.Rename(dir, cleanupDirName); err != nil {
		return err
	}

	return nil
}

func selectStateMachineLogSegment(segments []string, appliedIndex uint64) (int, error) {
	var index uint64

	for i := len(segments) - 1; i >= 0; i-- {
		segment := segments[i]

		if !strings.HasSuffix(segment, ".sml") {
			continue
		}

		if _, err := fmt.Sscanf(segment, "%20d.sml", &index); err != nil {
			return -1, err
		}

		if appliedIndex >= index {
			return i, nil
		}
	}

	return -1, nil
}
