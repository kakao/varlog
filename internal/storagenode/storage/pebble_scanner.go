package storage

import (
	"io"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"

	"github.com/kakao/varlog/proto/varlogpb"
)

type pebbleScanner struct {
	iter   *pebble.Iterator
	db     *pebble.DB
	logger *zap.Logger

	lowerKeyBuf *commitKeyBuffer
	upperKeyBuf *commitKeyBuffer
}

var _ Scanner = (*pebbleScanner)(nil)

func (scanner *pebbleScanner) Next() ScanResult {
	if !scanner.iter.Valid() {
		return NewInvalidScanResult(io.EOF)
	}
	ck := scanner.iter.Key()
	dk := scanner.iter.Value()
	data, closer, err := scanner.db.Get(dk)
	if err != nil {
		return NewInvalidScanResult(err)
	}
	defer func() {
		if err := closer.Close(); err != nil {
			scanner.logger.Warn("error while closing scanner", zap.Error(err))
		}
	}()
	logEntry := varlogpb.LogEntry{
		LogEntryMeta: varlogpb.LogEntryMeta{
			GLSN: decodeCommitKey(ck),
			LLSN: decodeDataKey(dk),
		},
	}
	if len(data) > 0 {
		logEntry.Data = make([]byte, len(data))
		copy(logEntry.Data, data)
	}
	scanner.iter.Next()
	return ScanResult{LogEntry: logEntry}
}

func (scanner *pebbleScanner) Close() error {
	scanner.lowerKeyBuf.release()
	scanner.upperKeyBuf.release()
	return scanner.iter.Close()
}
