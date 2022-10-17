package daemon

type config struct {
	args       []string
	envs       map[string]string
	outputChan chan<- string
}

func newConfig(opts []Option) (config, error) {
	var cfg config
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg, nil
}

// Option configures a Daemon.
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

// WithArguments sets command line arguments for a daemon.
func WithArguments(args ...string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.args = args
	})
}

// WithEnvironments sets environment variables for a daemon.
func WithEnvironments(envs map[string]string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.envs = envs
	})
}

// WithOutputChannel sets a channel to receive stdout and stderr.
func WithOutputChannel(outputChan chan<- string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.outputChan = outputChan
	})
}
