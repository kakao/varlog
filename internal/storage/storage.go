package storage

import (
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

type Scanner interface {
	Next() (varlog.LogEntry, error)
}

type Storage interface {
	Read(glsn types.GLSN) ([]byte, error)
	Scan(glsn types.GLSN) (Scanner, error)
	Write(llsn types.LLSN, data []byte) error
	Commit(llsn types.LLSN, glsn types.GLSN) error
	Delete(glsn types.GLSN) (uint64, error)
}
