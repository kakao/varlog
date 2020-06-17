package varlog

import (
	"errors"
	"sort"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

func (m *MetadataDescriptor) searchStorageNode(id types.StorageNodeID) (int, bool) {
	i := sort.Search(len(m.StorageNodes), func(i int) bool {
		return m.StorageNodes[i].StorageNodeId >= id
	})

	if i < len(m.StorageNodes) && m.StorageNodes[i].StorageNodeId == id {
		return i, true
	}

	return i, false
}

func (m *MetadataDescriptor) searchLogStream(id types.LogStreamID) (int, bool) {
	i := sort.Search(len(m.LogStreams), func(i int) bool {
		return m.LogStreams[i].LogStreamId >= id
	})

	if i < len(m.LogStreams) && m.LogStreams[i].LogStreamId == id {
		return i, true
	}

	return i, false
}

func (m *MetadataDescriptor) insertStorageNodeAt(idx int, sn *StorageNodeDescriptor) {
	l := m.StorageNodes
	l = append(l, StorageNodeDescriptor{})
	copy(l[idx+1:], l[idx:])

	l[idx] = *sn
	m.StorageNodes = l
}

func (m *MetadataDescriptor) insertLogStreamAt(idx int, ls *LogStreamDescriptor) {
	l := m.LogStreams
	l = append(l, LogStreamDescriptor{})
	copy(l[idx+1:], l[idx:])

	l[idx] = *ls
	m.LogStreams = l
}

func (m *MetadataDescriptor) InsertStorageNode(sn *StorageNodeDescriptor) error {
	idx, match := m.searchStorageNode(sn.StorageNodeId)
	if match {
		return errors.New("already exist")
	}

	m.insertStorageNodeAt(idx, sn)
	return nil
}

func (m *MetadataDescriptor) DeleteStorageNode(id types.StorageNodeID) {
	idx, match := m.searchStorageNode(id)
	if match {
		l := m.StorageNodes

		copy(l[idx:], l[idx+1:])
		m.StorageNodes = l[:len(l)-1]
	}
}

func (m *MetadataDescriptor) GetStorageNode(id types.StorageNodeID) *StorageNodeDescriptor {
	idx, match := m.searchStorageNode(id)
	if match {
		return &m.StorageNodes[idx]
	}

	return nil
}

func (m *MetadataDescriptor) InsertLogStream(ls *LogStreamDescriptor) error {
	idx, match := m.searchLogStream(ls.LogStreamId)
	if match {
		return errors.New("already exist")
	}

	m.insertLogStreamAt(idx, ls)
	return nil
}

func (m *MetadataDescriptor) DeleteLogStream(id types.LogStreamID) {
	idx, match := m.searchLogStream(id)
	if match {
		l := m.LogStreams

		copy(l[idx:], l[idx+1:])
		m.LogStreams = l[:len(l)-1]
	}
}

func (m *MetadataDescriptor) GetLogStream(id types.LogStreamID) *LogStreamDescriptor {
	idx, match := m.searchLogStream(id)
	if match {
		return &m.LogStreams[idx]
	}

	return nil
}
