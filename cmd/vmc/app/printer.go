package app

import (
	"encoding/json"
	"fmt"
	"io"
)

type Printer interface {
	Print(w io.Writer, msg interface{}) error
}

type jsonPrinter struct {
}

func (p *jsonPrinter) Print(w io.Writer, msg interface{}) error {
	if msg == nil {
		return nil
	}
	buf, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, string(buf))
	return nil
}
