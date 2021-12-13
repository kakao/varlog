package varlogctl

import (
	"encoding/json"
	"io"

	"github.com/kakao/varlog/internal/varlogctl/result"
)

func Print(res *result.Result, pretty bool, writer io.Writer) error {
	var b []byte
	var err error
	if pretty {
		b, err = json.MarshalIndent(res, "", "\t")
	} else {
		b, err = json.Marshal(res)
	}
	if err != nil {
		return err
	}
	_, err = writer.Write(b)
	return err
}
