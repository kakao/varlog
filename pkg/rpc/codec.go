package rpc

import (
	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto" //nolint:staticcheck
	"google.golang.org/grpc/encoding"
)

const name = "proto"

type codec struct{}

var _ encoding.Codec = codec{}

func init() {
	encoding.RegisterCodec(codec{})
}

func (codec) Marshal(v interface{}) ([]byte, error) {
	if m, ok := v.(gogoproto.Marshaler); ok {
		return m.Marshal()
	}
	return proto.Marshal(v.(proto.Message))
}

func (codec) Unmarshal(data []byte, v interface{}) error {
	if m, ok := v.(gogoproto.Unmarshaler); ok {
		return m.Unmarshal(data)
	}
	return proto.Unmarshal(data, v.(proto.Message))
}

func (codec) Name() string {
	return name
}
