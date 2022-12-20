package daemon

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// Daemon is an interface to provide simple methods for long-running processes.
type Daemon struct {
	config

	executable string
	cmd        *exec.Cmd
	proc       struct {
		p  *os.Process
		mu sync.Mutex
	}

	stdout io.ReadCloser
	stderr io.ReadCloser
	wg     sync.WaitGroup
}

// New creates the Daemon.
func New(executable string, opts ...Option) (*Daemon, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	d := &Daemon{
		config:     cfg,
		executable: executable,
	}

	d.cmd = exec.Command(d.executable, d.args...)
	if len(d.envs) > 0 {
		d.cmd.Env = append(d.cmd.Env, os.Environ()...)
		var sb strings.Builder
		for k, v := range d.envs {
			fmt.Fprintf(&sb, "%s=%s", k, v)
			d.cmd.Env = append(d.cmd.Env, sb.String())
			sb.Reset()
		}
	}

	if d.outputChan != nil {
		d.stdout, err = d.cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		d.stderr, err = d.cmd.StderrPipe()
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Run executes the daemon and waits for termination.
func (d *Daemon) Run() error {
	if err := d.cmd.Start(); err != nil {
		return err
	}
	d.proc.mu.Lock()
	d.proc.p = d.cmd.Process
	d.proc.mu.Unlock()

	done := make(chan struct{})

	if d.outputChan != nil {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			scanner := bufio.NewScanner(d.stdout)
			for scanner.Scan() {
				select {
				case d.outputChan <- scanner.Text():
				case <-done:
					return
				}
			}
		}()
	}
	if d.outputChan != nil {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			scanner := bufio.NewScanner(d.stderr)
			for scanner.Scan() {
				select {
				case d.outputChan <- scanner.Text():
				case <-done:
					return
				}
			}
		}()
	}
	defer close(done)
	return d.cmd.Wait()
}

// String returns a description of the daemon. Do not depend on its content.
func (d *Daemon) String() string {
	return d.cmd.String()
}

// Stop terminates the daemon.
func (d *Daemon) Stop() {
	d.proc.mu.Lock()
	if d.proc.p != nil {
		_ = d.proc.p.Signal(os.Interrupt)
	}
	d.proc.mu.Unlock()
	if d.stdout != nil {
		_ = d.stdout.Close()
	}
	if d.stderr != nil {
		_ = d.stderr.Close()
	}
	d.wg.Wait()
	if d.outputChan != nil {
		close(d.outputChan)
	}
}
