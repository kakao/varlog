//go:build tools
// +build tools

package tools

import (
	_ "github.com/golang/mock/gomock"
	_ "golang.org/x/lint/golint"
	_ "golang.org/x/tools/cmd/goimports"
	_ "golang.org/x/tools/cmd/stringer"
)
