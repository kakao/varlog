package demo

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/jsonpb"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/vmspb"
)

const (
	mrPort  = 21000
	snPort  = 9000
	vmsPort = 8190
)

var (
	mrAddr  = fmt.Sprintf("127.0.0.1:%d", mrPort)
	vmsAddr = fmt.Sprintf("127.0.0.1:%d", vmsPort)
)

func clearDir(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	for _, fi := range fis {
		if fi.IsDir() && fi.Name()[:4] == "raft" {
			t.Log(fi.Name())
			if err := os.RemoveAll(fi.Name()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func buildDir(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	dir = filepath.Join(dir, "../../../build")
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func runScanner(ctx context.Context, r io.ReadCloser, w io.Writer) {
	go func() {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() && ctx.Err() == nil {
			fmt.Fprintln(w, scanner.Text())
		}
	}()
}

func fork(ctx context.Context, args []string, envs []string, t *testing.T) (cmd *exec.Cmd, stdout io.ReadCloser, stderr io.ReadCloser) {
	bin := filepath.Join(buildDir(t), args[0])
	cmd = exec.CommandContext(ctx, bin, args[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err = cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(cmd.Env, envs...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, stdout, stderr
}

func forkMR(ctx context.Context, t *testing.T) *exec.Cmd {
	args := []string{
		"vmr", "start",
		"--bind", fmt.Sprintf("0.0.0.0:%d", mrPort),
	}
	cmd, stdout, stderr := fork(ctx, args, nil, t)
	runScanner(ctx, stdout, os.Stdout)
	runScanner(ctx, stderr, os.Stderr)
	return cmd
}

func forkVMS(ctx context.Context, t *testing.T) *exec.Cmd {
	args := []string{
		"vms", "start",
		"--mr-address", mrAddr,
		"--cluster-id", "1",
		"--rpc-bind-address", vmsAddr,
		"--replication-factor", "2",
	}
	cmd, stdout, stderr := fork(ctx, args, nil, t)
	runScanner(ctx, stdout, os.Stdout)
	runScanner(ctx, stderr, os.Stderr)
	return cmd
}

func forkSN(ctx context.Context, snid int, addr string, t *testing.T) *exec.Cmd {
	tmpdir := t.TempDir()
	args := []string{
		"vsn", "start",
		"--cluster-id", "1",
		"--snid", strconv.Itoa(snid),
		"--rpc-bind-address", addr,
		"--volumes", tmpdir,
	}
	cmd, stdout, stderr := fork(ctx, args, nil, t)
	runScanner(ctx, stdout, os.Stdout)
	runScanner(ctx, stderr, os.Stderr)
	return cmd
}

func addSN(ctx context.Context, addr string, t *testing.T) error {
	args := []string{
		"vmc", "add", "sn",
		"--storage-node-address", addr,
	}
	cmd, stdout, stderr := forkVMC(ctx, args, t)
	runScanner(ctx, stdout, os.Stdout)
	runScanner(ctx, stderr, os.Stderr)
	return cmd.Wait()
}

func addLS(ctx context.Context, t *testing.T) types.LogStreamID {
	args := []string{
		"vmc", "add", "ls",
	}
	cmd, stdout, stderr := forkVMC(ctx, args, t)
	runScanner(ctx, stderr, os.Stderr)
	var sb strings.Builder
	if _, err := io.Copy(&sb, stdout); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	var rsp vmspb.AddLogStreamResponse
	if err := jsonpb.UnmarshalString(sb.String(), &rsp); err != nil {
		t.Fatal(err)
	}
	return rsp.GetLogStream().GetLogStreamID()
}

func sealLS(ctx context.Context, logStreamID types.LogStreamID, t *testing.T) error {
	args := []string{
		"vmc", "seal", "ls",
		"--log-stream-id", fmt.Sprintf("%v", logStreamID),
	}
	cmd, stdout, stderr := forkVMC(ctx, args, t)
	runScanner(ctx, stderr, os.Stderr)
	runScanner(ctx, stdout, os.Stdout)
	return cmd.Wait()
}

func unsealLS(ctx context.Context, logStreamID types.LogStreamID, t *testing.T) error {
	args := []string{
		"vmc", "unseal", "ls",
		"--log-stream-id", fmt.Sprintf("%v", logStreamID),
	}
	cmd, stdout, stderr := forkVMC(ctx, args, t)
	runScanner(ctx, stderr, os.Stderr)
	runScanner(ctx, stdout, os.Stdout)
	return cmd.Wait()
}

func rmLS(ctx context.Context, logStreamID types.LogStreamID, t *testing.T) error {
	args := []string{
		"vmc", "rm", "ls",
		"--log-stream-id", fmt.Sprintf("%v", logStreamID),
	}
	cmd, stdout, stderr := forkVMC(ctx, args, t)
	runScanner(ctx, stderr, os.Stderr)
	runScanner(ctx, stdout, os.Stdout)
	return cmd.Wait()
}

func rmSN(ctx context.Context, storageNodeID types.StorageNodeID, t *testing.T) error {
	args := []string{
		"vmc", "rm", "sn",
		"--storage-node-id", fmt.Sprintf("%v", storageNodeID),
	}
	cmd, stdout, stderr := forkVMC(ctx, args, t)
	runScanner(ctx, stderr, os.Stderr)
	runScanner(ctx, stdout, os.Stdout)
	return cmd.Wait()
}

func forkVMC(ctx context.Context, args []string, t *testing.T) (*exec.Cmd, io.ReadCloser, io.ReadCloser) {
	envs := []string{"VMS_ADDRESS=" + vmsAddr}
	return fork(ctx, args, envs, t)
}

func TestDemo(t *testing.T) {
	clearDir(t)

	var snInfos = []struct {
		snid int
		addr string
	}{
		{
			snid: 1,
			addr: fmt.Sprintf("127.0.0.1:%d", snPort+1),
		},
		{
			snid: 2,
			addr: fmt.Sprintf("127.0.0.1:%d", snPort+2),
		},
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*10)
	defer cancel()

	// fork servers
	var cmds []*exec.Cmd
	for _, snInfo := range snInfos {
		cmds = append(cmds, forkSN(ctx, snInfo.snid, snInfo.addr, t))
	}
	cmds = append(cmds, forkMR(ctx, t))
	time.Sleep(3 * time.Second)
	cmds = append(cmds, forkVMS(ctx, t))
	time.Sleep(3 * time.Second)

	// ok: add storagenode
	for _, snInfo := range snInfos {
		if err := addSN(ctx, snInfo.addr, t); err != nil {
			t.Fatal(err)
		}
	}

	// error: add storagenode
	for _, snInfo := range snInfos {
		if err := addSN(ctx, snInfo.addr, t); err == nil {
			t.Fatal("shoud not be nil")
		}
	}

	// ok: add logstream
	logStreamID := addLS(ctx, t)

	// ok: seal logstream
	if err := sealLS(ctx, logStreamID, t); err != nil {
		t.Fatal(err)
	}

	// ok: unseal logstream
	if err := unsealLS(ctx, logStreamID, t); err != nil {
		t.Fatal(err)
	}

	// ok: seal logstream
	if err := sealLS(ctx, logStreamID, t); err != nil {
		t.Fatal(err)
	}

	// ok: rm logstream
	if err := rmLS(ctx, logStreamID, t); err != nil {
		t.Fatal(err)
	}

	for _, snInfo := range snInfos {
		if err := rmSN(ctx, types.StorageNodeID(snInfo.snid), t); err != nil {
			t.Fatal(err)
		}
	}

	cancel()
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Log(err)
		}
	}
}
