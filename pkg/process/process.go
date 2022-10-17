package process

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"sync"
)

// Process wraps exec.Cmd providing channels for stdout and stderr.
type Process struct {
	cmd *exec.Cmd

	wg     sync.WaitGroup
	cancel context.CancelFunc
	done   <-chan struct{}
	stdout *reader
	stderr *reader
}

// New returns the Process struct to execute a program specified by the
// arguments.
func New(ctx context.Context, name string, arg ...string) (*Process, error) {
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, name, arg...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		_ = stdout.Close()
		return nil, err
	}

	p := new(Process)
	p.cmd = cmd
	p.cancel = cancel
	p.done = ctx.Done()
	p.stdout = &reader{reader: stdout, ch: make(chan string)}
	p.stderr = &reader{reader: stderr, ch: make(chan string)}

	return p, nil
}

// Start starts the process, and it does not wait for the completion of the
// process. Users should call the Wait method to release system resources if it
// returns a nil result.
func (p *Process) Start() error {
	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		p.stdout.read(p.done)
	}()
	go func() {
		defer p.wg.Done()
		p.stderr.read(p.done)
	}()

	if err := p.cmd.Start(); err != nil {
		return err
	}
	return nil
}

// Stdout returns a channel for stdout strings.
func (p *Process) Stdout() <-chan string {
	return p.stdout.ch
}

// Stderr returns a channel for stderr strings.
func (p *Process) Stderr() <-chan string {
	return p.stderr.ch
}

// String returns a description of the process. Do not depend on its content.
func (p *Process) String() string {
	return p.cmd.String()
}

// Stop stops the process forcefully.
func (p *Process) Stop() {
	p.cancel()
	_ = p.stdout.reader.Close()
	_ = p.stderr.reader.Close()
}

// Wait waits for the process to complete.
func (p *Process) Wait() error {
	err := p.cmd.Wait()
	p.Stop()
	p.wg.Wait()
	return err
}

type reader struct {
	reader io.ReadCloser
	ch     chan string
}

func (rc *reader) read(done <-chan struct{}) {
	defer func() {
		close(rc.ch)
	}()
	scanner := bufio.NewScanner(rc.reader)
	for scanner.Scan() {
		select {
		case rc.ch <- scanner.Text():
		case <-done:
			return
		}
	}
}
