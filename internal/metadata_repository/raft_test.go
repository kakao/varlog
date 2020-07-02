package metadata_repository

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"testing"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type cluster struct {
	peers       []string
	commitC     []<-chan *string
	errorC      []<-chan error
	stateC      []<-chan raft.StateType
	proposeC    []chan string
	confChangeC []chan raftpb.ConfChange
}

// newCluster creates a cluster of n nodes
func newCluster(n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
	}

	clus := &cluster{
		peers:       peers,
		commitC:     make([]<-chan *string, len(peers)),
		errorC:      make([]<-chan error, len(peers)),
		stateC:      make([]<-chan raft.StateType, len(peers)),
		proposeC:    make([]chan string, len(peers)),
		confChangeC: make([]chan raftpb.ConfChange, len(peers)),
	}

	for i, peer := range clus.peers {
		url, err := url.Parse(peer)
		if err != nil {
			return nil
		}
		nodeID := types.NewNodeID(url.Host)

		os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
		os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))
		clus.proposeC[i] = make(chan string, 1)
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)
		logger, _ := zap.NewDevelopment()
		rc := newRaftNode(nodeID,
			clus.peers,
			false,
			nil,
			clus.proposeC[i],
			clus.confChangeC[i],
			logger,
		)

		clus.commitC[i] = rc.commitC
		clus.errorC[i] = rc.errorC
		clus.stateC[i] = rc.stateC

		go rc.startRaft()
	}

	return clus
}

// sinkReplay reads all commits in each node's local log.
func (clus *cluster) sinkReplay() {
	for i := range clus.peers {
		for s := range clus.commitC[i] {
			if s == nil {
				break
			}
		}
	}
}

// Close closes all cluster nodes and returns an error if any failed.
func (clus *cluster) Close() (err error) {
	for i := range clus.peers {
		close(clus.proposeC[i])
		for range clus.commitC[i] {
			// drain pending commits
		}
		// wait for channel to close
		if erri := <-clus.errorC[i]; erri != nil {
			err = erri
		}
		// clean intermediates
		os.RemoveAll(fmt.Sprintf("raft-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raft-%d-snap", i+1))
	}
	return err
}

func (clus *cluster) closeNoErrors(t *testing.T) {
	if err := clus.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProposeOnFollower(t *testing.T) {
	clus := newCluster(3)
	defer clus.closeNoErrors(t)

	clus.sinkReplay()

	vote := false

	donec := make(chan struct{})
	for i := range clus.peers {
		// feedback for "n" committed entries, then update donec
		go func(idx int, pC chan<- string, cC <-chan *string, eC <-chan error, sC <-chan raft.StateType) {
			var state raft.StateType

		Loop:
			for {
				select {
				case <-cC:
					break Loop
				case state = <-sC:
					log.Printf("[%d] state %#v", idx, state)
					if state == raft.StateLeader {
						vote = true
					}
				case err := <-eC:
					t.Errorf("eC message (%v)", err)
				}
			}
			donec <- struct{}{}
			for range cC {
				// acknowledge the commits from other nodes so
				// raft continues to make progress
			}
		}(i+1, clus.proposeC[i], clus.commitC[i], clus.errorC[i], clus.stateC[i])

		// one message feedback per node
		go func(i int) { clus.proposeC[i] <- fmt.Sprintf("%d", i+1) }(i)
	}

	for range clus.peers {
		<-donec
	}

	if vote != true {
		t.Error(vote)
	}
}

/*
// TestCloseProposerBeforeReplay tests closing the producer before raft starts.
func TestCloseProposerBeforeReplay(t *testing.T) {
	clus := newCluster(1)
	// close before replay so raft never starts
	defer clus.closeNoErrors(t)
}

// TestCloseProposerInflight tests closing the producer while
// committed messages are being published to the client.
func TestCloseProposerInflight(t *testing.T) {
	clus := newCluster(1)
	defer clus.closeNoErrors(t)

	clus.sinkReplay()

	// some inflight ops
	go func() {
		clus.proposeC[0] <- "foo"
		clus.proposeC[0] <- "bar"
	}()

	// wait for one message
	if c, ok := <-clus.commitC[0]; *c != "foo" || !ok {
		t.Fatalf("Commit failed")
	}
}
*/
