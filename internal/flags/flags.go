package flags

import (
	"time"

	"github.com/urfave/cli/v2"
)

type FlagDesc struct {
	Name        string
	Aliases     []string
	Usage       string
	Envs        []string
	DefaultText string
}

func (fd *FlagDesc) DurationFlag(required bool, defaultValue time.Duration) *cli.DurationFlag {
	return &cli.DurationFlag{
		Name:        fd.Name,
		Aliases:     fd.Aliases,
		Usage:       fd.Usage,
		EnvVars:     fd.Envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.DefaultText,
	}
}

func (fd *FlagDesc) Uint64Flag(required bool, defaultValue uint64) *cli.Uint64Flag {
	return &cli.Uint64Flag{
		Name:        fd.Name,
		Aliases:     fd.Aliases,
		Usage:       fd.Usage,
		EnvVars:     fd.Envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.DefaultText,
	}
}

func (fd *FlagDesc) IntFlag(required bool, defaultValue int) *cli.IntFlag {
	return &cli.IntFlag{
		Name:        fd.Name,
		Aliases:     fd.Aliases,
		Usage:       fd.Usage,
		EnvVars:     fd.Envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.DefaultText,
	}
}

func (fd *FlagDesc) UintFlag(required bool, defaultValue uint) *cli.UintFlag {
	return &cli.UintFlag{
		Name:        fd.Name,
		Aliases:     fd.Aliases,
		Usage:       fd.Usage,
		EnvVars:     fd.Envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.DefaultText,
	}
}

func (fd *FlagDesc) StringFlag(required bool, defaultValue string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        fd.Name,
		Aliases:     fd.Aliases,
		Usage:       fd.Usage,
		EnvVars:     fd.Envs,
		Required:    required,
		Value:       defaultValue,
		DefaultText: fd.DefaultText,
	}
}

func (fd *FlagDesc) StringSliceFlag(required bool, defaultValues []string) *cli.StringSliceFlag {
	return &cli.StringSliceFlag{
		Name:        fd.Name,
		Aliases:     fd.Aliases,
		Usage:       fd.Usage,
		EnvVars:     fd.Envs,
		Required:    required,
		Value:       cli.NewStringSlice(defaultValues...),
		DefaultText: fd.DefaultText,
	}
}

func (fd *FlagDesc) BoolFlag() *cli.BoolFlag {
	return &cli.BoolFlag{
		Name:        fd.Name,
		Aliases:     fd.Aliases,
		Usage:       fd.Usage,
		EnvVars:     fd.Envs,
		DefaultText: fd.DefaultText,
	}
}

func MetadataRepositoryAddress() *FlagDesc {
	return &FlagDesc{
		Name:    "metadata-repository-address",
		Aliases: []string{"mr-address", "metadata-repository", "mr"},
	}
}

func StorageNodeID() *FlagDesc {
	return &FlagDesc{
		Name:    "storage-node-id",
		Aliases: []string{"storage-node", "storagenode", "snid"},
	}
}

func TopicID() *FlagDesc {
	return &FlagDesc{
		Name:    "topic-id",
		Aliases: []string{"topic", "tpid"},
	}
}

func LogStreamID() *FlagDesc {
	return &FlagDesc{
		Name:    "log-stream-id",
		Aliases: []string{"logstream-id", "logstream", "lsid"},
	}
}
