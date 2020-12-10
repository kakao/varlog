package vtesting

import (
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap"
)

const defaultRaftTick = time.Millisecond * 100
const defaultCommitTick = time.Millisecond * 10
const defaultTimeoutUnit = time.Millisecond * 400
const defaultProcCount = 8
const defaultRaftDir = "raftdata"

var (
	testRaftDir     string        = defaultRaftDir
	testRaftTick    time.Duration = defaultRaftTick
	testCommitTick  time.Duration = defaultCommitTick
	testTimeoutUnit time.Duration = defaultTimeoutUnit
	testLogger      *zap.Logger   = zap.NewNop()
)

func init() {
	var v string
	v = os.Getenv("TEST_TIMEOUT_UNIT")
	if dur, err := time.ParseDuration(v); err == nil {
		testTimeoutUnit = dur
	}
	log.Printf("TEST_TIMEOUT_UNIT=%v", testTimeoutUnit)

	v = os.Getenv("TEST_RAFT_TICK")
	if dur, err := time.ParseDuration(v); err == nil {
		testRaftTick = dur
	}
	log.Printf("TEST_RAFT_TICK=%v", testRaftTick)

	v = os.Getenv("TEST_COMMIT_TICK")
	if dur, err := time.ParseDuration(v); err == nil {
		testCommitTick = dur
	}
	log.Printf("TEST_COMMIT_TICK=%v", testCommitTick)

	v = os.Getenv("TEST_USE_LOGGER")
	v = strings.ToLower(v)
	if v == "true" || v == "1" {
		lg, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		testLogger = lg
	}
	zap.ReplaceGlobals(testLogger)
	log.Printf("TEST_USE_LOGGER=%v", v)
}

func TimeoutUnitTimesFactor(factor int64) time.Duration {
	timeoutUnit := TimeoutAccordingToProcCnt(testTimeoutUnit)

	return time.Duration(int64(timeoutUnit) * factor)
}

func TimeoutAccordingToProcCnt(timeout time.Duration) time.Duration {
	procs := runtime.GOMAXPROCS(0)
	if procs < defaultProcCount {
		timeout += timeout * time.Duration(math.Log2(float64(defaultProcCount/procs)))
	}

	return timeout
}

func TestLogger() *zap.Logger {
	return testLogger
}

func TestRaftTick() time.Duration {
	return testRaftTick
}

func TestCommitTick() time.Duration {
	return testCommitTick
}

func TestRaftDir() string {
	return testRaftDir
}
