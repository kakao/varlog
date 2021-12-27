package vflag

import "github.com/urfave/cli/v2"

type FlagDescriptor struct {
	Name        string
	Aliases     []string
	EnvVars     []string
	Description string
	Required    bool
}

func (fd *FlagDescriptor) StringFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:     fd.Name,
		Aliases:  fd.Aliases,
		Usage:    fd.Description,
		EnvVars:  fd.EnvVars,
		Required: fd.Required,
	}
}

func (fd *FlagDescriptor) StringFlagV(defaultValue string) *cli.StringFlag {
	f := fd.StringFlag()
	f.Value = defaultValue
	return f
}

func (fd *FlagDescriptor) StringSliceFlag() *cli.StringSliceFlag {
	return &cli.StringSliceFlag{
		Name:     fd.Name,
		Aliases:  fd.Aliases,
		Usage:    fd.Description,
		EnvVars:  fd.EnvVars,
		Required: fd.Required,
	}
}

func (fd *FlagDescriptor) StringSliceFlagV(defaultValue []string) *cli.StringSliceFlag {
	f := fd.StringSliceFlag()
	f.Value = cli.NewStringSlice(defaultValue...)
	return f
}

func (fd *FlagDescriptor) IntFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:     fd.Name,
		Aliases:  fd.Aliases,
		Usage:    fd.Description,
		EnvVars:  fd.EnvVars,
		Required: fd.Required,
	}
}

func (fd *FlagDescriptor) IntFlagV(defaultValue int) *cli.IntFlag {
	f := fd.IntFlag()
	f.Value = defaultValue
	return f
}

func (fd *FlagDescriptor) BoolFlag() *cli.BoolFlag {
	return &cli.BoolFlag{
		Name:     fd.Name,
		Aliases:  fd.Aliases,
		Usage:    fd.Description,
		EnvVars:  fd.EnvVars,
		Required: fd.Required,
	}
}

func (fd *FlagDescriptor) DurationFlag() *cli.DurationFlag {
	return &cli.DurationFlag{
		Name:     fd.Name,
		Aliases:  fd.Aliases,
		Usage:    fd.Description,
		EnvVars:  fd.EnvVars,
		Required: fd.Required,
	}
}
