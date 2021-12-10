package storage

import (
	"io"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"

	"github.com/kakao/varlog/proto/varlogpb"
)

type pebbleScanner struct {
	scanConfig

	iter   *pebble.Iterator
	db     *pebble.DB
	logger *zap.Logger

	commitKeyBuffers struct {
		lower *commitKeyBuffer
		upper *commitKeyBuffer
	}

	dataKeyBuffers struct {
		lower *dataKeyBuffer
		upper *dataKeyBuffer
	}
}

var _ Scanner = (*pebbleScanner)(nil)

func (scanner *pebbleScanner) Next() ScanResult {
	if !scanner.iter.Valid() {
		return NewInvalidScanResult(io.EOF)
	}

	var (
		logEntry varlogpb.LogEntry
		err      error
	)
	if scanner.withGLSN {
		logEntry, err = scanner.nextGLSN()
	} else {
		logEntry, err = scanner.nextLLSN()
	}
	if err != nil {
		return NewInvalidScanResult(err)
	}
	scanner.iter.Next()
	return ScanResult{LogEntry: logEntry}
}

func (scanner *pebbleScanner) Close() error {
	if scanner.withGLSN {
		scanner.commitKeyBuffers.lower.release()
		scanner.commitKeyBuffers.upper.release()
	} else {
		scanner.dataKeyBuffers.lower.release()
		scanner.dataKeyBuffers.upper.release()
	}
	return scanner.iter.Close()
}

func (scanner *pebbleScanner) nextGLSN() (logEntry varlogpb.LogEntry, err error) {
	ck := scanner.iter.Key()
	dk := scanner.iter.Value()
	data, closer, err := scanner.db.Get(dk)
	if err != nil {
		return logEntry, err
	}
	defer func() {
		if err := closer.Close(); err != nil {
			scanner.logger.Warn("error while closing scanner", zap.Error(err))
		}
	}()
	logEntry.LogEntryMeta.GLSN = decodeCommitKey(ck)
	logEntry.LogEntryMeta.LLSN = decodeDataKey(dk)
	if len(data) > 0 {
		logEntry.Data = make([]byte, len(data))
		copy(logEntry.Data, data)
	}
	return logEntry, nil
}

func (scanner *pebbleScanner) nextLLSN() (logEntry varlogpb.LogEntry, err error) {
	dk := scanner.iter.Key()
	data := scanner.iter.Value()
	logEntry.LogEntryMeta.LLSN = decodeDataKey(dk)
	if len(data) > 0 {
		logEntry.Data = make([]byte, len(data))
		copy(logEntry.Data, data)
	}
	return logEntry, nil
}

type invalidPebbleScanner struct {
	err error
}

var _ Scanner = (*invalidPebbleScanner)(nil)

func (scanner *invalidPebbleScanner) Next() ScanResult {
	return NewInvalidScanResult(scanner.err)
}

func (scanner *invalidPebbleScanner) Close() error {
	return nil
}
