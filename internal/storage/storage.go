package storage

type Storage interface {
	Read(epoch uint64, glsn uint64) ([]byte, error)
	Append(epoch uint64, glsn uint64, data []byte) error
	Fill(epoch uint64, glsn uint64) error
	Trim(epoch uint64, glsn uint64) error
	Seal(epoch uint64, maxLsn *uint64) error
	GetHighLSN(lsn *uint64) error
	GetEpoch() uint64
}
