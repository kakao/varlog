package varlog

import (
	"errors"
	"sort"
)

func (meta *MetadataDescriptor) GetLastProjection() *ProjectionDescriptor {
	if meta == nil {
		return nil
	}

	prjs := meta.GetProjections()
	if len(prjs) == 0 {
		return nil
	}

	return &prjs[len(prjs)-1]
}

func (meta *MetadataDescriptor) GetLastEpoch() uint64 {
	prj := meta.GetLastProjection()
	return prj.GetEpoch()
}

func (meta *MetadataDescriptor) GetProjection(epoch uint64) *ProjectionDescriptor {
	lenProjections := len(meta.GetProjections())
	if lenProjections == 0 {
		return nil
	}

	idx := sort.Search(lenProjections, func(i int) bool {
		return meta.GetProjections()[i].GetEpoch() >= epoch
	})

	if idx < lenProjections &&
		meta.GetProjections()[idx].GetEpoch() == epoch {
		return &meta.GetProjections()[idx]
	}

	return nil
}

func (p *ProjectionDescriptor) GetLogStream(glsn uint64) (*LogStreamDescriptor, error) {
	lenLogStreams := uint64(len(p.GetLogStreams()))
	if lenLogStreams == 0 {
		return nil, errors.New("no logstream")
	}

	idx := glsn % lenLogStreams
	return &p.GetLogStreams()[idx], nil
}
