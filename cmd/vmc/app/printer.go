package app

import (
	"fmt"
	"io"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

type Printer interface {
	Print(w io.Writer, msg proto.Message) error
}

type jsonPrinter struct {
}

func (p *jsonPrinter) Print(w io.Writer, msg proto.Message) error {
	if msg == nil {
		return nil
	}
	marshaler := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: true,
		Indent:       "  ",
	}
	str, err := marshaler.MarshalToString(msg)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, str)
	return nil
}
