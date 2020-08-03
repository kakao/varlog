package types

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"sync/atomic"
)

type ClusterID uint32

type StorageNodeID uint32

type LogStreamID uint32

type GLSN uint64

const (
	InvalidGLSN = GLSN(0)
	MinGLSN     = GLSN(1)
	MaxGLSN     = GLSN(math.MaxUint64)
)

func (glsn GLSN) Invalid() bool {
	return glsn == InvalidGLSN
}

type AtomicGLSN uint64

func (glsn *AtomicGLSN) Add(delta uint64) GLSN {
	return GLSN(atomic.AddUint64((*uint64)(glsn), delta))
}

func (glsn *AtomicGLSN) Load() GLSN {
	return GLSN(atomic.LoadUint64((*uint64)(glsn)))
}

func (glsn *AtomicGLSN) Store(val GLSN) {
	atomic.StoreUint64((*uint64)(glsn), uint64(val))
}

func (glsn *AtomicGLSN) CompareAndSwap(old, new GLSN) (swapped bool) {
	swapped = atomic.CompareAndSwapUint64((*uint64)(glsn), uint64(old), uint64(new))
	return swapped
}

type LLSN uint64

const (
	InvalidLLSN = LLSN(0)
	MinLLSN     = LLSN(1)
	MaxLLSN     = LLSN(math.MaxUint64)
)

func (llsn LLSN) Invalid() bool {
	return llsn == InvalidLLSN
}

type AtomicLLSN uint64

func (llsn *AtomicLLSN) Add(delta uint64) LLSN {
	return LLSN(atomic.AddUint64((*uint64)(llsn), delta))
}

func (llsn *AtomicLLSN) Load() LLSN {
	return LLSN(atomic.LoadUint64((*uint64)(llsn)))
}

func (llsn *AtomicLLSN) Store(val LLSN) {
	atomic.StoreUint64((*uint64)(llsn), uint64(val))
}

func (glsn *AtomicLLSN) CompareAndSwap(old, new LLSN) (swapped bool) {
	swapped = atomic.CompareAndSwapUint64((*uint64)(glsn), uint64(old), uint64(new))
	return swapped
}

type NodeID uint64

const (
	InvalidNodeID = 0
	MinNodeID     = 1
	MaxNodeID     = math.MaxUint64
)

// convert string(ip:port) to uint64
// TODO:: LookupHost
func NewNodeID(addr string) NodeID {
	var id uint64 = 0

	host, sport, err := net.SplitHostPort(addr)
	if err != nil {
		return InvalidNodeID
	}

	port, err := strconv.Atoi(sport)
	if err != nil {
		return InvalidNodeID
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return InvalidNodeID
	}
	ipv4 := ip.To4()
	if ipv4 == nil {
		return InvalidNodeID
	}
	for i := 0; i < net.IPv4len; i++ {
		offset := uint32((net.IPv4len + 3 - i) * 8)

		id |= uint64(ipv4[i]) << offset
	}
	id |= (uint64(port) & 0xffff) << 16
	return NodeID(id)
}

func (nid NodeID) String() string {
	return strconv.FormatUint(uint64(nid), 10)
}

func (nid NodeID) Reverse() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d",
		(nid&0xff00000000000000)>>56,
		(nid&0x00ff000000000000)>>48,
		(nid&0x0000ff0000000000)>>40,
		(nid&0x000000ff00000000)>>32,
		(nid&0x00000000ffff0000)>>16)
}
