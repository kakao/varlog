package types

import "math"

type StorageNodeID int32

type LogStreamID int32

const (
	InvalidLogStreamID = LogStreamID(math.MinInt32)
)

type GLSN uint64

type LLSN uint64
