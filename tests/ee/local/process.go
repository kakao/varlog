package local

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Process struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc

	stdout        io.ReadCloser
	stdoutScanner *bufio.Scanner

	stderr        io.ReadCloser
	stderrScanner *bufio.Scanner
}

func NewProcess(t *testing.T, name string, args []string, envs []string) *Process {
	p := &Process{}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.cmd = exec.CommandContext(ctx, name, args...)
	p.cmd.Env = append(p.cmd.Env, envs...)

	var err error
	p.stdout, err = p.cmd.StdoutPipe()
	assert.NoError(t, err)
	p.stdoutScanner = bufio.NewScanner(p.stdout)

	p.stderr, err = p.cmd.StderrPipe()
	assert.NoError(t, err)
	p.stderrScanner = bufio.NewScanner(p.stderr)

	return p
}

func (p *Process) Start(t *testing.T) {
	err := p.cmd.Start()
	assert.NoError(t, err)
}

func (p *Process) Wait() {
	_ = p.cmd.Wait()
}

func (p *Process) Stop() {
	p.cancel()
}

func (p *Process) NextStdoutLine() (string, bool) {
	return p.nextLine(p.stdoutScanner)
}

func (p *Process) NextStderrLine() (string, bool) {
	return p.nextLine(p.stderrScanner)
}

func (p *Process) nextLine(scanner *bufio.Scanner) (string, bool) {
	if scanner.Scan() {
		return scanner.Text(), true
	}
	return "", false
}
