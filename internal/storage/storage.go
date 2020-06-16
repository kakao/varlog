package storage

import (
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

type Storage interface {
	Read(glsn types.GLSN) ([]byte, error)
	Write(glsn types.GLSN, data []byte) error
	Delete(glsn types.GLSN) error
}
