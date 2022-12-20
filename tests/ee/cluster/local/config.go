package local

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kakao/varlog/tests/ee/cluster"
)

const (
	defaultMRPortBase = 10001
	defaultAdminAddr  = "127.0.0.1:9093"
	defaultSNPortBase = 20001
)

var defaultGrpcHealthProbeExecutable = flag.String("grpc-health-probe-executable", "", "executable path for grpc_health_probe")

type config struct {
	cluster.Config

	mrExecutable string
	mrPortBase   int

	adminExecutable string
	adminAddr       string

	snExecutable string
	snPortBase   int

	grpcHealthProbeExecutable string
}

func newConfig(opts []Option) (config, error) {
	if len(*defaultGrpcHealthProbeExecutable) == 0 {
		*defaultGrpcHealthProbeExecutable, _ = exec.LookPath("grpc-health-probe")
		if len(*defaultGrpcHealthProbeExecutable) == 0 {
			*defaultGrpcHealthProbeExecutable, _ = exec.LookPath("grpc_health_probe")
		}
	}

	cfg := config{
		mrExecutable:              filepath.Join(binDir(), "vmr"),
		mrPortBase:                defaultMRPortBase,
		adminExecutable:           filepath.Join(binDir(), "varlogadm"),
		adminAddr:                 defaultAdminAddr,
		snExecutable:              filepath.Join(binDir(), "varlogsn"),
		snPortBase:                defaultSNPortBase,
		grpcHealthProbeExecutable: *defaultGrpcHealthProbeExecutable,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	cfg.ensureDefault()
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg *config) ensureDefault() {
	if abs, err := filepath.Abs(cfg.mrExecutable); err == nil {
		cfg.mrExecutable = abs
	}
	if abs, err := filepath.Abs(cfg.adminExecutable); err == nil {
		cfg.adminExecutable = abs
	}
	if abs, err := filepath.Abs(cfg.snExecutable); err == nil {
		cfg.snExecutable = abs
	}
	if abs, err := filepath.Abs(cfg.grpcHealthProbeExecutable); err == nil {
		cfg.grpcHealthProbeExecutable = abs
	}
}

func (cfg *config) validate() error {
	if err := validateExecutable(cfg.mrExecutable); err != nil {
		return fmt.Errorf("cluster: mr executable %s: %w", cfg.mrExecutable, err)
	}
	if err := validateExecutable(cfg.adminExecutable); err != nil {
		return fmt.Errorf("cluster: admin executable %s: %w", cfg.adminExecutable, err)
	}
	if err := validateExecutable(cfg.snExecutable); err != nil {
		return fmt.Errorf("cluster: sn executable %s: %w", cfg.snExecutable, err)
	}
	if err := validateExecutable(cfg.grpcHealthProbeExecutable); err != nil {
		return fmt.Errorf("cluster: grpc-health-probe executable %s: %w", cfg.grpcHealthProbeExecutable, err)
	}
	return nil
}

func validateExecutable(executable string) error {
	if len(executable) == 0 {
		return errors.New("no path")
	}
	if !filepath.IsAbs(executable) {
		return errors.New("not absolute path")
	}
	info, err := os.Stat(executable)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("not regular")
	}
	if info.Mode()&os.FileMode(0100) == 0 {
		return errors.New("permission: not executable")
	}
	return nil
}

type Option interface {
	apply(*config)
}

type funcOption struct {
	f func(*config)
}

func newFuncOption(f func(*config)) *funcOption {
	return &funcOption{f: f}
}

func (fo *funcOption) apply(cfg *config) {
	fo.f(cfg)
}

func WithCommonConfig(commonConfig cluster.Config) Option {
	return newFuncOption(func(cfg *config) {
		cfg.Config = commonConfig
	})
}

func WithMetadataRepositoryExecutable(executable string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.mrExecutable = executable
	})
}

func WithAdminExecutable(executable string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.adminExecutable = executable
	})
}

func WithStorageNodeExecutable(executable string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snExecutable = executable
	})
}

func WithGRPCHealthProbeExecutable(executable string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.grpcHealthProbeExecutable = executable
	})
}
