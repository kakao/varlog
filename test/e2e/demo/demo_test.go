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
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/proto/vmspb"
	"github.com/kakao/varlog/vtesting"
)

func getClearDir(t *testing.T, raftAddr string) func() {
	return func() {
		dir, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}

		fis, err := ioutil.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}

		for _, fi := range fis {
			if fi.IsDir() && (fi.Name() == "raftdata" || fi.Name() == "log") {
				if err := os.RemoveAll(fi.Name()); err != nil {
					t.Fatal(err)
				}
			}
		}

		nodeID := types.NewNodeIDFromURL(raftAddr)
		os.RemoveAll(fmt.Sprintf("%s/wal/%d", vtesting.TestRaftDir(), nodeID))
		os.RemoveAll(fmt.Sprintf("%s/snap/%d", vtesting.TestRaftDir(), nodeID))
	}
}

func binDir(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	dir = filepath.Join(dir, "../../../bin")
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
	bin := filepath.Join(binDir(t), args[0])
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

func forkMR(ctx context.Context, rpcAddr string, raftAddr string, t *testing.T) *exec.Cmd {
	args := []string{
		"vmr", "start",
		"--bind", rpcAddr,
		"--raft-address", raftAddr,
		"--raft-dir", vtesting.TestRaftDir(),
	}
	cmd, stdout, stderr := fork(ctx, args, nil, t)
	runScanner(ctx, stdout, os.Stdout)
	runScanner(ctx, stderr, os.Stderr)
	return cmd
}

func forkVMS(ctx context.Context, vmsAddr string, mrAddr string, t *testing.T) *exec.Cmd {
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

func addSN(ctx context.Context, addr string, vmsAddr string, t *testing.T) error {
	args := []string{
		"vmc", "add", "sn",
		"--storage-node-address", addr,
		"--rpc-timeout", "3s",
	}
	cmd, stdout, stderr := forkVMC(ctx, args, vmsAddr, t)
	runScanner(ctx, stdout, os.Stdout)
	runScanner(ctx, stderr, os.Stderr)
	return cmd.Wait()
}

func addLS(ctx context.Context, vmsAddr string, t *testing.T) types.LogStreamID {
	args := []string{
		"vmc", "add", "ls",
	}
	cmd, stdout, stderr := forkVMC(ctx, args, vmsAddr, t)
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

func sealLS(ctx context.Context, logStreamID types.LogStreamID, vmsAddr string, t *testing.T) error {
	args := []string{
		"vmc", "seal", "ls",
		"--log-stream-id", fmt.Sprintf("%v", logStreamID),
	}
	cmd, stdout, stderr := forkVMC(ctx, args, vmsAddr, t)
	runScanner(ctx, stderr, os.Stderr)
	runScanner(ctx, stdout, os.Stdout)
	return cmd.Wait()
}

func unsealLS(ctx context.Context, logStreamID types.LogStreamID, vmsAddr string, t *testing.T) error {
	args := []string{
		"vmc", "unseal", "ls",
		"--log-stream-id", fmt.Sprintf("%v", logStreamID),
	}
	cmd, stdout, stderr := forkVMC(ctx, args, vmsAddr, t)
	runScanner(ctx, stderr, os.Stderr)
	runScanner(ctx, stdout, os.Stdout)
	return cmd.Wait()
}

func rmLS(ctx context.Context, logStreamID types.LogStreamID, vmsAddr string, t *testing.T) error {
	args := []string{
		"vmc", "rm", "ls",
		"--log-stream-id", fmt.Sprintf("%v", logStreamID),
	}
	cmd, stdout, stderr := forkVMC(ctx, args, vmsAddr, t)
	runScanner(ctx, stderr, os.Stderr)
	runScanner(ctx, stdout, os.Stdout)
	return cmd.Wait()
}

func rmSN(ctx context.Context, storageNodeID types.StorageNodeID, vmsAddr string, t *testing.T) error {
	args := []string{
		"vmc", "rm", "sn",
		"--storage-node-id", fmt.Sprintf("%v", storageNodeID),
	}
	cmd, stdout, stderr := forkVMC(ctx, args, vmsAddr, t)
	runScanner(ctx, stderr, os.Stderr)
	runScanner(ctx, stdout, os.Stdout)
	return cmd.Wait()
}

func forkVMC(ctx context.Context, args []string, vmsAddr string, t *testing.T) (*exec.Cmd, io.ReadCloser, io.ReadCloser) {
	envs := []string{"VMS_ADDRESS=" + vmsAddr}
	return fork(ctx, args, envs, t)
}

func TestDemo(t *testing.T) {
	portLease, err := ports.ReserveWeaklyWithRetry(10000)
	if err != nil {
		t.Fatal(err)
	}
	defer portLease.Release()

	nextPort := portLease.Base()

	var snInfos = []struct {
		snid int
		addr string
	}{
		{
			snid: 1,
			addr: fmt.Sprintf("127.0.0.1:%d", nextPort),
		},
		{
			snid: 2,
			addr: fmt.Sprintf("127.0.0.1:%d", nextPort+1),
		},
	}
	nextPort += 2

	mrRPCAddr := fmt.Sprintf("127.0.0.1:%d", nextPort)
	nextPort++

	mrRAFTAddr := fmt.Sprintf("http://127.0.0.1:%d", nextPort)
	nextPort++

	vmsAddr := fmt.Sprintf("127.0.0.1:%d", nextPort)
	nextPort++

	cleanup := getClearDir(t, mrRAFTAddr)
	cleanup()
	t.Cleanup(cleanup)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*10)
	defer cancel()

	// fork servers
	var cmds []*exec.Cmd
	for _, snInfo := range snInfos {
		cmds = append(cmds, forkSN(ctx, snInfo.snid, snInfo.addr, t))
	}

	cmds = append(cmds, forkMR(ctx, mrRPCAddr, mrRAFTAddr, t))
	time.Sleep(10 * time.Second)

	cmds = append(cmds, forkVMS(ctx, vmsAddr, mrRPCAddr, t))
	time.Sleep(10 * time.Second)

	// ok: add storagenode
	for _, snInfo := range snInfos {
		if err := addSN(ctx, snInfo.addr, vmsAddr, t); err != nil {
			t.Fatal(err)
		}
	}

	// error: add storagenode
	for _, snInfo := range snInfos {
		if err := addSN(ctx, snInfo.addr, vmsAddr, t); err == nil {
			t.Fatal("shoud not be nil")
		}
	}

	// ok: add logstream
	logStreamID := addLS(ctx, vmsAddr, t)

	// ok: seal logstream
	if err := sealLS(ctx, logStreamID, vmsAddr, t); err != nil {
		t.Fatal(err)
	}

	// ok: unseal logstream
	if err := unsealLS(ctx, logStreamID, vmsAddr, t); err != nil {
		t.Fatal(err)
	}

	// ok: seal logstream
	if err := sealLS(ctx, logStreamID, vmsAddr, t); err != nil {
		t.Fatal(err)
	}

	// ok: rm logstream
	if err := rmLS(ctx, logStreamID, vmsAddr, t); err != nil {
		t.Fatal(err)
	}

	for _, snInfo := range snInfos {
		if err := rmSN(ctx, types.StorageNodeID(snInfo.snid), vmsAddr, t); err != nil {
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
