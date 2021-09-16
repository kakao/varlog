package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
)

type ClusterID uint32

var _ fmt.Stringer = (*ClusterID)(nil)

func NewClusterIDFromUint(u uint) (ClusterID, error) {
	if u > math.MaxUint32 {
		return 0, fmt.Errorf("cluster id overflow %v", u)
	}
	return ClusterID(u), nil
}

func ParseClusterID(s string) (ClusterID, error) {
	id, err := strconv.ParseUint(s, 10, 32)
	return ClusterID(id), err
}

func (cid ClusterID) String() string {
	return strconv.FormatUint(uint64(cid), 10)
}

type StorageNodeID int32

var _ fmt.Stringer = (*StorageNodeID)(nil)

func RandomStorageNodeID() StorageNodeID {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return StorageNodeID(r.Int31())
}

func ParseStorageNodeID(s string) (StorageNodeID, error) {
	id, err := strconv.ParseInt(s, 10, 32)
	return StorageNodeID(id), err
}

func (snid StorageNodeID) String() string {
	return strconv.FormatInt(int64(snid), 10)
}

type LogStreamID int32

const MaxLogStreamID = math.MaxInt32

var _ fmt.Stringer = (*LogStreamID)(nil)

func ParseLogStreamID(s string) (LogStreamID, error) {
	id, err := strconv.ParseInt(s, 10, 32)
	return LogStreamID(id), err
}

func (lsid LogStreamID) String() string {
	return strconv.FormatInt(int64(lsid), 10)
}

type TopicID int32

var _ fmt.Stringer = (*TopicID)(nil)

func ParseTopicID(s string) (TopicID, error) {
	id, err := strconv.ParseInt(s, 10, 32)
	return TopicID(id), err
}

func (tpid TopicID) String() string {
	return strconv.FormatInt(int64(tpid), 10)
}

type Version uint64

const (
	InvalidVersion = Version(0)
	MinVersion     = Version(1)
	MaxVersion     = Version(math.MaxUint64)
)

var VersionLen = binary.Size(InvalidVersion)

func (ver Version) Invalid() bool {
	return ver == InvalidVersion
}

type AtomicVersion uint64

func (ver *AtomicVersion) Add(delta uint64) Version {
	return Version(atomic.AddUint64((*uint64)(ver), delta))
}

func (ver *AtomicVersion) Load() Version {
	return Version(atomic.LoadUint64((*uint64)(ver)))
}

func (ver *AtomicVersion) Store(val Version) {
	atomic.StoreUint64((*uint64)(ver), uint64(val))
}

func (ver *AtomicVersion) CompareAndSwap(old, new Version) (swapped bool) {
	swapped = atomic.CompareAndSwapUint64((*uint64)(ver), uint64(old), uint64(new))
	return swapped
}

type GLSN uint64

const (
	InvalidGLSN = GLSN(0)
	MinGLSN     = GLSN(1)
	MaxGLSN     = GLSN(math.MaxUint64)
)

var GLSNLen = binary.Size(InvalidGLSN)

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

var LLSNLen = binary.Size(InvalidLLSN)

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

func (llsn *AtomicLLSN) CompareAndSwap(old, new LLSN) (swapped bool) {
	swapped = atomic.CompareAndSwapUint64((*uint64)(llsn), uint64(old), uint64(new))
	return swapped
}

type NodeID uint64

const (
	InvalidNodeID = NodeID(0)
	MinNodeID     = NodeID(1)
	MaxNodeID     = NodeID(math.MaxUint64)
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

func NewNodeIDFromURL(rawurl string) NodeID {
	u, err := url.Parse(rawurl)
	if err != nil {
		return InvalidNodeID
	}
	return NewNodeID(u.Host)
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
