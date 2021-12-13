package main

import (
	"time"

	"github.com/urfave/cli/v2"
)

type flagDesc struct {
	name        string
	aliases     []string
	usage       string
	envs        []string
	defaultText string
}

func (fd *flagDesc) DurationFlag(required bool, defaultValue time.Duration) *cli.DurationFlag {
	return &cli.DurationFlag{
		Name:        fd.name,
		Aliases:     fd.aliases,
		Usage:       fd.usage,
		EnvVars:     fd.envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.defaultText,
	}
}

func (fd *flagDesc) Uint64Flag(required bool, defaultValue uint64) *cli.Uint64Flag {
	return &cli.Uint64Flag{
		Name:        fd.name,
		Aliases:     fd.aliases,
		Usage:       fd.usage,
		EnvVars:     fd.envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.defaultText,
	}
}

func (fd *flagDesc) IntFlag(required bool, defaultValue int) *cli.IntFlag {
	return &cli.IntFlag{
		Name:        fd.name,
		Aliases:     fd.aliases,
		Usage:       fd.usage,
		EnvVars:     fd.envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.defaultText,
	}
}

func (fd *flagDesc) StringFlag(required bool, defaultValue string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        fd.name,
		Aliases:     fd.aliases,
		Usage:       fd.usage,
		EnvVars:     fd.envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.defaultText,
	}
}

func (fd *flagDesc) BoolFlag() *cli.BoolFlag {
	return &cli.BoolFlag{
		Name:        fd.name,
		Aliases:     fd.aliases,
		Usage:       fd.usage,
		EnvVars:     fd.envs,
		DefaultText: fd.defaultText,
	}
}
