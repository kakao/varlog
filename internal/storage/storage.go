package storage

import (
	"github.com/kakao/varlog/pkg/varlog/types"
)

type Storage interface {
	Read(glsn types.GLSN) ([]byte, error)
	Write(llsn types.LLSN, data []byte) error
	Commit(llsn types.LLSN, glsn types.GLSN) error
	Delete(glsn types.GLSN) error
}
