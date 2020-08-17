package metadata_repository

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type cluster struct {
	peers       []string
	commitC     []<-chan *raftCommittedEntry
	errorC      []<-chan error
	stateC      []<-chan raft.StateType
	proposeC    []chan string
	confChangeC []chan raftpb.ConfChange
	running     []bool
}

// newCluster creates a cluster of n nodes
func newCluster(n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
	}

	clus := &cluster{
		peers:       peers,
		commitC:     make([]<-chan *raftCommittedEntry, len(peers)),
		errorC:      make([]<-chan error, len(peers)),
		stateC:      make([]<-chan raft.StateType, len(peers)),
		proposeC:    make([]chan string, len(peers)),
		confChangeC: make([]chan raftpb.ConfChange, len(peers)),
		running:     make([]bool, len(peers)),
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
		//logger, _ := zap.NewDevelopment()
		logger := zap.NewNop()
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
		clus.running[i] = true

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

func (clus *cluster) close(i int) (err error) {
	if !clus.running[i] {
		return nil
	}

	close(clus.proposeC[i])
	for range clus.commitC[i] {
		// drain pending commits
	}
	// wait for channel to close
	if erri := <-clus.errorC[i]; erri != nil {
		err = erri
	}
	// clean intermediates
	url, _ := url.Parse(clus.peers[i])
	nodeID := types.NewNodeID(url.Host)

	os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
	os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))

	clus.running[i] = false

	return
}

// Close closes all cluster nodes and returns an error if any failed.
func (clus *cluster) Close() (err error) {
	for i := range clus.peers {
		if erri := clus.close(i); erri != nil {
			err = erri
		}
	}

	return
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
		go func(idx int, pC chan<- string, cC <-chan *raftCommittedEntry, eC <-chan error, sC <-chan raft.StateType) {
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

func TestFailoverLeaderElection(t *testing.T) {
	Convey("Given Raft Cluster", t, func(ctx C) {
		clus := newCluster(3)
		defer clus.closeNoErrors(t)

		clus.sinkReplay()

		voteC := make(chan int)
		cancels := make([]context.CancelFunc, 3)

		var wg sync.WaitGroup

		for i := range clus.peers {
			wg.Add(1)
			ctx, cancel := context.WithCancel(context.TODO())
			cancels[i] = cancel
			// feedback for "n" committed entries, then update donec
			go func(ctx context.Context, idx int, pC chan<- string, cC <-chan *raftCommittedEntry, eC <-chan error, sC <-chan raft.StateType) {
				defer wg.Done()
				var state raft.StateType

			Loop:
				for {
					select {
					case <-ctx.Done():
						break Loop
					case <-cC:
					case state = <-sC:
						if state == raft.StateLeader {
							voteC <- idx
						}
					case <-eC:
					}
				}
			}(ctx, i, clus.proposeC[i], clus.commitC[i], clus.errorC[i], clus.stateC[i])
		}

		leader := -1
		select {
		case leader = <-voteC:
		case <-time.After(time.Second):
		}

		So(leader, ShouldBeGreaterThan, -1)

		Convey("When leader crash", func(ctx C) {
			cancels[leader]()
			clus.close(leader)

			Convey("Then raft should elect", func(ctx C) {
				leader := -1
				select {
				case leader = <-voteC:
				case <-time.After(time.Second):
				}
				So(leader, ShouldBeGreaterThan, -1)

				for i := range clus.peers {
					cancels[i]()
				}

				wg.Wait()
			})
		})
	})
}
