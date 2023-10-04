package varlogpb

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
)

const (
	logStreamStatusRunning   = "running"
	logStreamStatusSealing   = "sealing"
	logStreamStatusSealed    = "sealed"
	logStreamStatusDeleted   = "deleted"
	logStreamStatusUnsealing = "unsealing"
)

// MarshalJSON returns the JSON encoding of the LogStreamStatus.
func (lss LogStreamStatus) MarshalJSON() ([]byte, error) {
	var s string
	switch lss {
	case LogStreamStatusRunning:
		s = logStreamStatusRunning
	case LogStreamStatusSealing:
		s = logStreamStatusSealing
	case LogStreamStatusSealed:
		s = logStreamStatusSealed
	case LogStreamStatusDeleted:
		s = logStreamStatusDeleted
	case LogStreamStatusUnsealing:
		s = logStreamStatusUnsealing
	default:
		return nil, fmt.Errorf("unexpected logstream status: %v", lss)
	}
	return json.Marshal(s)
}

// UnmarshalJSON parses the JSON-encoded data and stores the result in the value of type
// LogStreamStatus.
func (lss *LogStreamStatus) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch strings.ToLower(s) {
	case logStreamStatusRunning:
		*lss = LogStreamStatusRunning
	case logStreamStatusSealing:
		*lss = LogStreamStatusSealing
	case logStreamStatusSealed:
		*lss = LogStreamStatusSealed
	case logStreamStatusDeleted:
		*lss = LogStreamStatusDeleted
	case logStreamStatusUnsealing:
		*lss = LogStreamStatusUnsealing
	default:
		return fmt.Errorf("unexpected data: %s", s)
	}
	return nil
}

func (s LogStreamStatus) Deleted() bool {
	return s == LogStreamStatusDeleted
}

func (s LogStreamStatus) Running() bool {
	return s == LogStreamStatusRunning
}

func (s LogStreamStatus) Sealed() bool {
	return s == LogStreamStatusSealing || s == LogStreamStatusSealed
}

func (s StorageNodeStatus) Running() bool {
	return s == StorageNodeStatusRunning
}

func (s StorageNodeStatus) Deleted() bool {
	return s == StorageNodeStatusDeleted
}

func (s TopicStatus) Deleted() bool {
	return s == TopicStatusDeleted
}

func (lsd LogStreamDescriptor) Validate() error {
	if len(lsd.Replicas) == 0 {
		return errors.New("log stream descriptor: no replicas")
	}

	const size = 3
	snids := make(map[types.StorageNodeID]struct{}, size)
	for idx := range lsd.Replicas {
		if lsd.Replicas[idx] == nil {
			return errors.New("log stream descriptor: nil replica")
		}
		if err := lsd.Replicas[idx].Validate(); err != nil {
			return fmt.Errorf("log stream descriptor: %w", err)
		}
		snids[lsd.Replicas[idx].StorageNodeID] = struct{}{}
	}
	if len(snids) != len(lsd.Replicas) {
		return errors.New("log stream descriptor: duplicate storage nodes")
	}
	return nil
}

func (l *LogStreamDescriptor) Valid() bool {
	if l == nil || len(l.Replicas) == 0 {
		return false
	}

	for _, r := range l.Replicas {
		if !r.valid() {
			return false
		}
	}

	return true
}

func (l *LogStreamDescriptor) IsReplica(snID types.StorageNodeID) bool {
	for _, r := range l.GetReplicas() {
		if r.StorageNodeID == snID {
			return true
		}
	}

	return false
}

func (rd ReplicaDescriptor) Validate() error {
	if rd.StorageNodeID.Invalid() {
		return errors.New("replica descriptor: invalid storage node id")
	}
	if len(rd.StorageNodePath) == 0 {
		return fmt.Errorf("replica descriptor: no path in storage node %d", rd.StorageNodeID)
	}
	return nil
}

func (r *ReplicaDescriptor) valid() bool {
	return r != nil && len(r.StorageNodePath) != 0
}

func (m *MetadataDescriptor) Must() *MetadataDescriptor {
	if m == nil {
		panic("MetadataDescriptor is nil")
	}
	return m
}

func (m *MetadataDescriptor) searchStorageNode(id types.StorageNodeID) (int, bool) {
	i := sort.Search(len(m.StorageNodes), func(i int) bool {
		return m.StorageNodes[i].StorageNodeID >= id
	})

	if i < len(m.StorageNodes) && m.StorageNodes[i].StorageNodeID == id {
		return i, true
	}

	return i, false
}

func (m *MetadataDescriptor) searchLogStream(id types.LogStreamID) (int, bool) {
	i := sort.Search(len(m.LogStreams), func(i int) bool {
		return m.LogStreams[i].LogStreamID >= id
	})

	if i < len(m.LogStreams) && m.LogStreams[i].LogStreamID == id {
		return i, true
	}

	return i, false
}

func (m *MetadataDescriptor) insertStorageNodeAt(idx int, sn *StorageNodeDescriptor) {
	l := m.StorageNodes
	l = append(l, &StorageNodeDescriptor{})
	copy(l[idx+1:], l[idx:])

	l[idx] = sn
	m.StorageNodes = l
}

func (m *MetadataDescriptor) insertLogStreamAt(idx int, ls *LogStreamDescriptor) {
	l := m.LogStreams
	l = append(l, &LogStreamDescriptor{})
	copy(l[idx+1:], l[idx:])

	l[idx] = ls
	m.LogStreams = l
}

func (m *MetadataDescriptor) updateStorageNodeAt(idx int, sn *StorageNodeDescriptor) {
	m.StorageNodes[idx] = sn
}

func (m *MetadataDescriptor) updateLogStreamAt(idx int, ls *LogStreamDescriptor) {
	m.LogStreams[idx] = ls
}

func (m *MetadataDescriptor) InsertStorageNode(sn *StorageNodeDescriptor) error {
	if m == nil || sn == nil {
		return nil
	}

	idx, match := m.searchStorageNode(sn.StorageNodeID)
	if match {
		return errors.New("already exist")
	}

	m.insertStorageNodeAt(idx, sn)
	return nil
}

func (m *MetadataDescriptor) UpdateStorageNode(sn *StorageNodeDescriptor) error {
	if m == nil || sn == nil {
		return errors.New("invalid argument")
	}

	idx, match := m.searchStorageNode(sn.StorageNodeID)
	if !match {
		return errors.New("not exist")
	}

	m.updateStorageNodeAt(idx, sn)
	return nil
}

func (m *MetadataDescriptor) UpsertStorageNode(sn *StorageNodeDescriptor) error {
	if err := m.InsertStorageNode(sn); err != nil {
		return m.UpdateStorageNode(sn)
	}

	return nil
}

func (m *MetadataDescriptor) DeleteStorageNode(id types.StorageNodeID) error {
	if m == nil {
		return nil
	}

	idx, match := m.searchStorageNode(id)
	if !match {
		return errors.New("not exists")
	}

	l := m.StorageNodes

	copy(l[idx:], l[idx+1:])
	m.StorageNodes = l[:len(l)-1]

	return nil
}

// GetStorageNode finds the storage node specified by the argument id.
// It returns nil if the storage node does not exist.
func (m *MetadataDescriptor) GetStorageNode(id types.StorageNodeID) *StorageNodeDescriptor {
	if m == nil {
		return nil
	}

	idx, match := m.searchStorageNode(id)
	if match {
		return m.StorageNodes[idx]
	}

	return nil
}

func (m *MetadataDescriptor) HaveStorageNode(id types.StorageNodeID) (*StorageNodeDescriptor, error) {
	if m == nil {
		return nil, errors.New("MetadataDescriptor is nil")
	}
	if snd := m.GetStorageNode(id); snd != nil {
		return snd, nil
	}
	return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
}

func (m *MetadataDescriptor) MustHaveStorageNode(id types.StorageNodeID) (*StorageNodeDescriptor, error) {
	return m.Must().HaveStorageNode(id)
}

func (m *MetadataDescriptor) NotHaveStorageNode(id types.StorageNodeID) error {
	if m == nil {
		return errors.New("MetadataDescriptor is nil")
	}
	if snd := m.GetStorageNode(id); snd == nil {
		return nil
	}
	return errors.Wrap(verrors.ErrExist, "storage node")
}

func (m *MetadataDescriptor) MustNotHaveStorageNode(id types.StorageNodeID) error {
	return m.Must().NotHaveStorageNode(id)
}

func (m *MetadataDescriptor) InsertLogStream(ls *LogStreamDescriptor) error {
	if m == nil || ls == nil {
		return nil
	}

	idx, match := m.searchLogStream(ls.LogStreamID)
	if match {
		return errors.New("already exist")
	}

	m.insertLogStreamAt(idx, ls)
	return nil
}

func (m *MetadataDescriptor) UpdateLogStream(ls *LogStreamDescriptor) error {
	if m == nil || ls == nil {
		return errors.New("not exist")
	}

	idx, match := m.searchLogStream(ls.LogStreamID)
	if !match {
		return errors.New("not exist")
	}

	m.updateLogStreamAt(idx, ls)
	return nil
}

func (m *MetadataDescriptor) UpsertLogStream(ls *LogStreamDescriptor) error {
	if err := m.InsertLogStream(ls); err != nil {
		return m.UpdateLogStream(ls)
	}

	return nil
}

func (m *MetadataDescriptor) DeleteLogStream(id types.LogStreamID) error {
	if m == nil {
		return nil
	}

	idx, match := m.searchLogStream(id)
	if !match {
		return errors.New("not exists")
	}

	l := m.LogStreams

	copy(l[idx:], l[idx+1:])
	m.LogStreams = l[:len(l)-1]

	return nil
}

func (m *MetadataDescriptor) GetLogStream(id types.LogStreamID) *LogStreamDescriptor {
	if m == nil {
		return nil
	}

	idx, match := m.searchLogStream(id)
	if match {
		return m.LogStreams[idx]
	}

	return nil
}

func (m *MetadataDescriptor) HaveLogStream(id types.LogStreamID) (*LogStreamDescriptor, error) {
	if m == nil {
		return nil, errors.New("MetadataDescriptor is nil")
	}
	if lsd := m.GetLogStream(id); lsd != nil {
		return lsd, nil
	}
	return nil, errors.Wrap(verrors.ErrNotExist, "log stream")
}

func (m *MetadataDescriptor) MustHaveLogStream(id types.LogStreamID) (*LogStreamDescriptor, error) {
	return m.Must().HaveLogStream(id)
}

func (m *MetadataDescriptor) NotHaveLogStream(id types.LogStreamID) error {
	if m == nil {
		return errors.New("MetadataDescriptor is nil")
	}
	if lsd := m.GetLogStream(id); lsd == nil {
		return nil
	}
	return errors.Wrap(verrors.ErrExist, "log stream")
}

func (m *MetadataDescriptor) MustNotHaveLogStream(id types.LogStreamID) error {
	return m.Must().NotHaveLogStream(id)
}

func (m *MetadataDescriptor) GetReplicasByStorageNodeID(id types.StorageNodeID) []*ReplicaDescriptor {
	if m == nil {
		return nil
	}
	hint := len(m.GetLogStreams()) / (len(m.GetStorageNodes()) + 1)
	ret := make([]*ReplicaDescriptor, 0, hint)
	for _, lsd := range m.GetLogStreams() {
		for _, rd := range lsd.GetReplicas() {
			if rd.GetStorageNodeID() == id {
				ret = append(ret, rd)
			}
		}
	}
	return ret
}

func (m *MetadataDescriptor) searchTopic(id types.TopicID) (int, bool) {
	i := sort.Search(len(m.Topics), func(i int) bool {
		return m.Topics[i].TopicID >= id
	})

	if i < len(m.Topics) && m.Topics[i].TopicID == id {
		return i, true
	}

	return i, false
}

func (m *MetadataDescriptor) insertTopicAt(idx int, topic *TopicDescriptor) {
	l := m.Topics
	l = append(l, &TopicDescriptor{})
	copy(l[idx+1:], l[idx:])

	l[idx] = topic
	m.Topics = l
}

func (m *MetadataDescriptor) updateTopicAt(idx int, topic *TopicDescriptor) {
	m.Topics[idx] = topic
}

func (m *MetadataDescriptor) GetTopic(id types.TopicID) *TopicDescriptor {
	if m == nil {
		return nil
	}

	idx, match := m.searchTopic(id)
	if match {
		return m.Topics[idx]
	}

	return nil
}

func (m *MetadataDescriptor) InsertTopic(topic *TopicDescriptor) error {
	if m == nil || topic == nil {
		return nil
	}

	idx, match := m.searchTopic(topic.TopicID)
	if match {
		return errors.New("already exist")
	}

	m.insertTopicAt(idx, topic)
	return nil
}

func (m *MetadataDescriptor) DeleteTopic(id types.TopicID) error {
	if m == nil {
		return nil
	}

	idx, match := m.searchTopic(id)
	if !match {
		return errors.New("not exists")
	}

	l := m.Topics

	copy(l[idx:], l[idx+1:])
	m.Topics = l[:len(l)-1]

	return nil
}

func (m *MetadataDescriptor) UpdateTopic(topic *TopicDescriptor) error {
	if m == nil || topic == nil {
		return errors.New("not exist")
	}

	idx, match := m.searchTopic(topic.TopicID)
	if !match {
		return errors.New("not exist")
	}

	m.updateTopicAt(idx, topic)
	return nil
}

func (m *MetadataDescriptor) UpsertTopic(topic *TopicDescriptor) error {
	if err := m.InsertTopic(topic); err != nil {
		return m.UpdateTopic(topic)
	}

	return nil
}

func (m *MetadataDescriptor) HaveTopic(id types.TopicID) (*TopicDescriptor, error) {
	if m == nil {
		return nil, errors.New("MetadataDescriptor is nil")
	}
	if tnd := m.GetTopic(id); tnd != nil {
		return tnd, nil
	}
	return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
}

func (m *MetadataDescriptor) MustHaveTopic(id types.TopicID) (*TopicDescriptor, error) {
	return m.Must().HaveTopic(id)
}

func (m *MetadataDescriptor) NotHaveTopic(id types.TopicID) error {
	if m == nil {
		return errors.New("MetadataDescriptor is nil")
	}
	if tnd := m.GetTopic(id); tnd == nil {
		return nil
	}
	return errors.Wrap(verrors.ErrExist, "storage node")
}

func (m *MetadataDescriptor) MustNotHaveTopic(id types.TopicID) error {
	return m.Must().NotHaveTopic(id)
}

func (t *TopicDescriptor) searchLogStream(id types.LogStreamID) (int, bool) {
	i := sort.Search(len(t.LogStreams), func(i int) bool {
		return t.LogStreams[i] >= id
	})

	if i < len(t.LogStreams) && t.LogStreams[i] == id {
		return i, true
	}

	return i, false
}

func (t *TopicDescriptor) insertLogStreamAt(idx int, lsID types.LogStreamID) {
	l := t.LogStreams
	l = append(l, types.LogStreamID(0))
	copy(l[idx+1:], l[idx:])

	l[idx] = lsID
	t.LogStreams = l
}

func (t *TopicDescriptor) InsertLogStream(lsID types.LogStreamID) {
	if t == nil {
		return
	}

	idx, match := t.searchLogStream(lsID)
	if match {
		return
	}

	t.insertLogStreamAt(idx, lsID)
}

func (t *TopicDescriptor) HasLogStream(lsID types.LogStreamID) bool {
	if t == nil {
		return false
	}

	_, match := t.searchLogStream(lsID)
	return match
}

func (lsn LogSequenceNumber) Invalid() bool {
	return lsn.LLSN.Invalid() || lsn.GLSN.Invalid()
}
