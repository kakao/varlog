package rpc

import (
	gogoproto "github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
)

const name = "proto"

type codec struct{}

var _ encoding.Codec = codec{}

func init() {
	encoding.RegisterCodec(codec{})
}

func (codec) Marshal(v interface{}) ([]byte, error) {
	if m, ok := v.(gogoproto.Marshaler); ok {
		if m, ok := v.(gogoproto.Message); ok {
			return gogoproto.Marshal(m)
		}
		return m.Marshal()
	}
	return proto.Marshal(v.(proto.Message))
}

func (codec) Unmarshal(data []byte, v interface{}) error {
	if m, ok := v.(gogoproto.Unmarshaler); ok {
		if m, ok := v.(gogoproto.Message); ok {
			return gogoproto.Unmarshal(data, m)
		}
		return m.Unmarshal(data)
	}
	return proto.Unmarshal(data, v.(proto.Message))
}

func (codec) Name() string {
	return name
}
