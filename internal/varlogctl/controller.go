package varlogctl

import (
	"context"
	"encoding/json"
	"io"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

type ExecuteFunc func(context.Context, varlog.Admin) (any, error)

type Controller struct {
	config
}

func New(opts ...Option) (*Controller, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	ctl := &Controller{config: cfg}
	return ctl, nil
}

func (c *Controller) Execute(ctx context.Context) (any, error) {
	return c.executeFunc(ctx, c.admin)
}

func (c *Controller) Decode(res any) ([]byte, error) {
	if err, ok := res.(error); ok {
		panic(err)
	}
	if c.pretty {
		return json.MarshalIndent(res, "", "\t")
	}
	return json.Marshal(res)
}

func (c *Controller) Print(buf []byte, writer io.Writer) error {
	_, err := writer.Write(buf)
	return err
}
