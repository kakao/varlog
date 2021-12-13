package varlogpb

import (
	"sort"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
)

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

func (s *StorageDescriptor) valid() bool {
	return s != nil && len(s.Path) != 0 && s.Used <= s.Total
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

func (r *ReplicaDescriptor) valid() bool {
	return r != nil && len(r.Path) != 0
}

func DiffReplicaDescriptorSet(xs []*ReplicaDescriptor, ys []*ReplicaDescriptor) []*ReplicaDescriptor {
	xss := makeReplicaDescriptorDiffSet(xs)
	yss := makeReplicaDescriptorDiffSet(ys)
	for s := range yss {
		delete(xss, s)
	}
	if len(xss) == 0 {
		return nil
	}
	ret := make([]*ReplicaDescriptor, 0, len(xss))
	for _, x := range xss {
		ret = append(ret, x)
	}
	return ret
}

func makeReplicaDescriptorDiffSet(replicas []*ReplicaDescriptor) map[string]*ReplicaDescriptor {
	set := make(map[string]*ReplicaDescriptor, len(replicas))
	for _, replica := range replicas {
		set[replica.String()] = replica
	}
	return set
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

func (m *MetadataDescriptor) GetLogStreamsByStorageNodeID(id types.StorageNodeID) []*ReplicaDescriptor {
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

func (snmd *StorageNodeMetadataDescriptor) GetLogStream(logStreamID types.LogStreamID) (LogStreamMetadataDescriptor, bool) {
	logStreams := snmd.GetLogStreams()
	for i := range logStreams {
		if logStreams[i].GetLogStreamID() == logStreamID {
			return logStreams[i], true
		}
	}
	return LogStreamMetadataDescriptor{}, false
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
