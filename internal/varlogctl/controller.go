package varlogctl

import (
	"context"
	"io"

	"github.com/kakao/varlog/internal/varlogctl/result"
	"github.com/kakao/varlog/pkg/varlog"
)

type ExecuteFunc func(context.Context, varlog.Admin) *result.Result

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

func (c *Controller) Execute(ctx context.Context) (res *result.Result) {
	return c.executeFunc(ctx, c.admin)
}

func (c *Controller) Print(res *result.Result, writer io.Writer) error {
	return Print(res, c.pretty, writer)
}
