package rpc

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

const name = "proto"

type gogoprotoMessage interface {
	MarshalToSizedBuffer([]byte) (int, error)
	Unmarshal([]byte) error
	ProtoSize() int
}

var pool = mem.DefaultBufferPool()

type codec struct {
	fallback encoding.CodecV2
}

var _ encoding.CodecV2 = &codec{}

func init() {
	encoding.RegisterCodecV2(&codec{
		fallback: encoding.GetCodecV2(name),
	})
}

func (c *codec) Marshal(v any) (mem.BufferSlice, error) {
	if m, ok := v.(gogoprotoMessage); ok {
		size := m.ProtoSize()
		if mem.IsBelowBufferPoolingThreshold(size) {
			buf := make([]byte, size)
			if _, err := m.MarshalToSizedBuffer(buf[:size]); err != nil {
				return nil, err
			}
			return mem.BufferSlice{mem.SliceBuffer(buf)}, nil
		}

		buf := pool.Get(size)
		if _, err := m.MarshalToSizedBuffer((*buf)[:size]); err != nil {
			pool.Put(buf)
			return nil, err
		}
		return mem.BufferSlice{mem.NewBuffer(buf, pool)}, nil
	}
	return c.fallback.Marshal(v)
}

func (c *codec) Unmarshal(data mem.BufferSlice, v any) error {
	if m, ok := v.(gogoprotoMessage); ok {
		buf := data.MaterializeToBuffer(pool)
		defer buf.Free()
		return m.Unmarshal(buf.ReadOnlyData())
	}
	return c.fallback.Unmarshal(data, v)
}

func (*codec) Name() string {
	return name
}
