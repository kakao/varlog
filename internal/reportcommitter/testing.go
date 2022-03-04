package reportcommitter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/proto/snpb"
)

func TestNewClient(t *testing.T, addr string) (Client, func()) {
	client, err := NewClient(context.Background(), addr)
	assert.NoError(t, err)
	closer := func() {
		assert.NoError(t, client.Close())
	}
	return client, closer
}

func TestCommit(t *testing.T, addr string, cr snpb.CommitRequest) {
	client, closer := TestNewClient(t, addr)
	defer closer()
	assert.NoError(t, client.Commit(cr))
}

func TestGetReport(t *testing.T, addr string) []snpb.LogStreamUncommitReport {
	client, closer := TestNewClient(t, addr)
	defer closer()
	rsp, err := client.GetReport()
	assert.NoError(t, err)
	return rsp.UncommitReports
}
