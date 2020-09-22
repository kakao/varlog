package metadata_repository

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	varlogtypes "github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/netutil"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
	pb "github.com/kakao/varlog/proto/metadata_repository"

	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

// A key-value stream backed by raft
type raftNode struct {
	proposeC    chan string              // proposed messages from app
	confChangeC chan raftpb.ConfChange   // proposed cluster config changes
	commitC     chan *raftCommittedEntry // entries committed to app
	snapshotC   chan struct{}            // snapshot trigger

	id          varlogtypes.NodeID // node ID for raft
	bpeers      []string           // raft bootstrap peer URLs
	url         string             // raft listen url
	membership  *raftMembership    // raft membership
	raftTick    time.Duration      // raft tick
	join        bool               // node is joining an existing cluster
	waldir      string             // path to WAL directory
	snapdir     string             // path to snapshot directory
	getSnapshot func() ([]byte, *raftpb.ConfState, uint64)
	lastIndex   uint64 // index of log at start

	raftState     raft.StateType
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter *snap.Snapshotter
	snapCount   uint64

	transport *rafthttp.Transport

	logger *zap.Logger

	runner *runner.Runner
	cancel context.CancelFunc

	httprunner *runner.Runner
	httpcancel context.CancelFunc
}

// committed entry to app
type raftCommittedEntry struct {
	entryType raftpb.EntryType
	index     uint64
	data      []byte
	confState *raftpb.ConfState
}

type raftMembership struct {
	state   raft.StateType                // raft state
	leader  types.ID                      // raft leader
	members map[varlogtypes.NodeID]string // raft member map
	peers   map[varlogtypes.NodeID]string // raft known peer map
	mu      sync.RWMutex
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries.
func newRaftNode(id varlogtypes.NodeID, peers []string, join bool, snapCount uint64, getSnapshot func() ([]byte, *raftpb.ConfState, uint64), proposeC chan string,
	confChangeC chan raftpb.ConfChange, logger *zap.Logger) *raftNode {

	commitC := make(chan *raftCommittedEntry)
	snapshotC := make(chan struct{})

	rc := &raftNode{
		proposeC:    proposeC,
		snapshotC:   snapshotC,
		confChangeC: confChangeC,
		commitC:     commitC,
		id:          id,
		bpeers:      peers,
		membership:  newRaftMemebership(),
		raftTick:    10 * time.Millisecond,
		join:        join,
		waldir:      fmt.Sprintf("raft-%d", id),
		snapdir:     fmt.Sprintf("raft-%d-snap", id),
		getSnapshot: getSnapshot,
		snapCount:   snapCount,
		logger:      logger,
		runner:      runner.New("raft-node", logger),
		httprunner:  runner.New("http", logger),
	}

	return rc
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		rc.logger.Panic("first index of committed entry should <= progress.appliedIndex+1",
			zap.Uint64("firstIdx", firstIdx),
			zap.Uint64("appliedIndex", rc.appliedIndex),
		)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ctx context.Context, ents []raftpb.Entry) bool {
	for i := range ents {
		var cs *raftpb.ConfState
		data := ents[i].Data

		if ents[i].Type == raftpb.EntryConfChange {
			var cc raftpb.ConfChange
			err := cc.Unmarshal(ents[i].Data)
			if err != nil {
				rc.logger.Panic("ConfChange Unmarshal fail", zap.String("err", err.Error()))
			}

			cs = rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) == 0 {
					cc.Context = rc.membership.getPeer(varlogtypes.NodeID(cc.NodeID))
					if cc.Context == nil {
						rc.logger.Panic("unknown peer", zap.Uint64("nodeID", cc.NodeID))
					}

					data, err = cc.Marshal()
					if err != nil {
						rc.logger.Panic("ConfChange re-marshal fail", zap.String("err", err.Error()))
					}
				}

				if cc.NodeID != uint64(rc.id) {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
				rc.membership.addMember(varlogtypes.NodeID(cc.NodeID), string(cc.Context))

			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					rc.logger.Info("I've been removed from the cluster! Shutting down.")
					return false
				}

				if rc.membership.removePeer(varlogtypes.NodeID(cc.NodeID)) {
					rc.transport.RemovePeer(types.ID(cc.NodeID))
				}
			}
		}

		if len(data) > 0 &&
			(ents[i].Type == raftpb.EntryNormal ||
				ents[i].Type == raftpb.EntryConfChange) {
			e := &raftCommittedEntry{
				entryType: ents[i].Type,
				index:     ents[i].Index,
				data:      data,
				confState: cs,
			}

			select {
			case rc.commitC <- e:
			case <-ctx.Done():
				return false
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		//TODO:: check neccessary whether send signal replay WAL complete
		/*
			// special nil commit to signal replay has finished
			if ents[i].Index == rc.lastIndex {
				fmt.Printf("[%v] publishEntry:: signal load snap[%v]\n", rc.id, len(ents))
				select {
				case rc.commitC <- nil:
				case <-ctx.Done():
					return false
				}
			}
		*/
	}
	return true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		rc.logger.Panic("error loading snapshot", zap.String("err", err.Error()))
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			rc.logger.Panic("cannot create dir for wal", zap.String("err", err.Error()))
		}

		w, err := wal.Create(rc.logger.Named("wal"), rc.waldir, nil)
		if err != nil {
			rc.logger.Panic("create wal error", zap.String("err", err.Error()))
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	rc.logger.Info("loading WAL",
		zap.Uint64("term", walsnap.Term),
		zap.Uint64("index", walsnap.Index),
	)
	w, err := wal.Open(rc.logger.Named("wal"), rc.waldir, walsnap)
	if err != nil {
		rc.logger.Panic("error loading wal", zap.String("err", err.Error()))
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	rc.logger.Info("replaying WAL", zap.Uint64("member", uint64(rc.id)))
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		rc.logger.Panic("failed to read wal", zap.String("err", err.Error()))
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
		rc.publishSnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	//TODO:: WAL replay to state machine

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)

	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	}
	//TODO:: check neccessary whether send signal replay WAL complete
	return w
}

func (rc *raftNode) start() {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			rc.logger.Panic("cannot create dir for snapshot", zap.String("err", err.Error()))
		}
	}
	rc.snapshotter = snap.New(rc.logger.Named("snapshot"), rc.snapdir)
	snapshot := rc.loadSnapshot()

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL(snapshot)

	rpeers := make([]raft.Peer, len(rc.bpeers))
	for i, peer := range rc.bpeers {
		url, err := url.Parse(peer)
		if err != nil {
			rc.logger.Panic("invalid peer name",
				zap.String("peer", peer),
				zap.String("err", err.Error()),
			)
		}

		nodeID := varlogtypes.NewNodeID(url.Host)
		if nodeID == varlogtypes.InvalidNodeID {
			rc.logger.Panic("invalid peer",
				zap.String("peer", peer),
			)
		}

		rpeers[i] = raft.Peer{ID: uint64(nodeID)}
	}

	c := &raft.Config{
		Logger:                    NewRaftLogger(rc.logger.Named("core")),
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldwal || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger.Named("transport"),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		Snapshotter: rc.snapshotter,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.FormatUint(uint64(rc.id), 10)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()

	if snapshot == nil {
		for i, peer := range rpeers {
			if peer.ID == uint64(rc.id) {
				rc.url = rc.bpeers[i]
			} else {
				rc.transport.AddPeer(types.ID(peer.ID), []string{rc.bpeers[i]})
			}
			rc.membership.addPeer(varlogtypes.NodeID(peer.ID), rc.bpeers[i])
		}
	} else {
		rc.recoverMembership(*snapshot)
	}

	httpctx, httpcancel := rc.httprunner.WithManagedCancel(context.Background())
	rc.httpcancel = httpcancel
	if err := rc.httprunner.RunC(httpctx, rc.runRaft); err != nil {
		rc.logger.Panic("could not run", zap.Error(err))
	}

	ctx, cancel := rc.runner.WithManagedCancel(context.Background())
	rc.cancel = cancel
	if err := rc.runner.RunC(ctx, rc.processSnapshot); err != nil {
		rc.logger.Panic("could not run", zap.Error(err))
	}
	if err := rc.runner.RunC(ctx, rc.processRaftEvent); err != nil {
		rc.logger.Panic("could not run", zap.Error(err))
	}
	if err := rc.runner.RunC(ctx, rc.processPropose); err != nil {
		rc.logger.Panic("could not run", zap.Error(err))
	}
}

func (rc *raftNode) longestConnected() (types.ID, bool) {
	var longest types.ID
	var oldest time.Time

	membs := rc.membership.getMembers()
	for _, id := range membs {
		if types.ID(rc.id) == id {
			continue
		}

		tm := rc.transport.ActiveSince(id)
		if tm.IsZero() { // inactive
			continue
		}

		if oldest.IsZero() { // first longest candidate
			oldest = tm
			longest = id
		}

		if tm.Before(oldest) {
			oldest = tm
			longest = id
		}
	}
	if uint64(longest) == 0 {
		return longest, false
	}
	return longest, true
}

func (rc *raftNode) transferLeadership(wait bool) error {
	if !rc.membership.isLeader() {
		return nil
	}

	transferee, ok := rc.longestConnected()
	if !ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*rc.raftTick)
	defer cancel()

	rc.node.TransferLeadership(ctx, uint64(rc.membership.getLeader()), uint64(transferee))
	for wait && uint64(rc.membership.getLeader()) != uint64(transferee) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(rc.raftTick):
		}
	}

	return nil
}

func (rc *raftNode) stop(transfer bool) {
	if transfer {
		// for leader election test without transferring leader
		if err := rc.transferLeadership(true); err != nil {
			rc.logger.Warn("transfer leader fail", zap.Uint64("ID", uint64(rc.id)))
		}
	}

	rc.cancel()
	rc.runner.Stop()

	if rc.wal != nil {
		rc.wal.Close()
	}

	close(rc.commitC)
	close(rc.snapshotC)
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stopRaft() {
	rc.stopHTTP()
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.httpcancel()
	rc.transport.Stop()
	rc.httprunner.Stop()
}

type snapReaderCloser struct{ *bytes.Reader }

func (s snapReaderCloser) Close() error { return nil }

func (rc *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {

			snapshot, _ := rc.raftStorage.Snapshot()
			if !raft.IsEmptySnap(snapshot) {
				ms[i].Snapshot = snapshot
				sm := snap.NewMessage(ms[i], snapReaderCloser{bytes.NewReader(nil)}, 0)

				//TODO:: concurrency limit
				rc.runner.Run(func(context.Context) {
					rc.transport.SendSnapshot(*sm)
				})

				ms[i].To = 0
			}
		}
	}

	return ms
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	rc.logger.Info("publishing snapshot",
		zap.Uint64("Idx", snapshotToSave.Metadata.Index),
		zap.Uint64("Term", snapshotToSave.Metadata.Term),
		zap.Uint64("preSnapIdx", atomic.LoadUint64(&rc.snapshotIndex)),
		zap.Uint64("appliedIdx", rc.appliedIndex),
		zap.Int("voter", len(snapshotToSave.Metadata.ConfState.Voters)))

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		rc.logger.Panic("snapshot index should > progress.appliedIndex",
			zap.Uint64("snapshotIndex", snapshotToSave.Metadata.Index),
			zap.Uint64("appliedIndex", rc.appliedIndex),
		)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	atomic.StoreUint64(&rc.snapshotIndex, snapshotToSave.Metadata.Index)
	rc.appliedIndex = snapshotToSave.Metadata.Index

	rc.logger.Info("finish snapshot",
		zap.Uint64("snapIdx", snapshotToSave.Metadata.Index),
		zap.Uint64("appliedIdx", rc.appliedIndex))
}

func (rc *raftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-atomic.LoadUint64(&rc.snapshotIndex) <= rc.snapCount {
		return
	}

	select {
	case rc.snapshotC <- struct{}{}:
	default:
	}
}

func (rc *raftNode) doSnapshot() {
	snapshotIndex := atomic.LoadUint64(&rc.snapshotIndex)

	data, confState, appliedIndex := rc.getSnapshot()
	if snapshotIndex >= appliedIndex {
		return
	}

	rc.logger.Info("start snapshot",
		zap.Uint64("snapshotAppliedIndex", appliedIndex),
		zap.Uint64("lastSnapshotIndex", snapshotIndex),
	)

	snap, err := rc.raftStorage.CreateSnapshot(appliedIndex, confState, data)
	if err != nil {
		rc.logger.Panic(err.Error())
	}

	if err := rc.saveSnap(snap); err != nil {
		rc.logger.Panic(err.Error())
	}

	compactIndex := uint64(1)
	if appliedIndex > DefaultSnapshotCatchUpEntriesN {
		compactIndex = appliedIndex - DefaultSnapshotCatchUpEntriesN
	}

	if err := rc.raftStorage.Compact(compactIndex); err != nil && err != raft.ErrCompacted {
		rc.logger.Panic("storage compact fail", zap.String("err", err.Error()))
	}

	rc.logger.Info("compacted log", zap.Uint64("index", compactIndex))
	atomic.StoreUint64(&rc.snapshotIndex, appliedIndex)
}

func (rc *raftNode) processSnapshot(ctx context.Context) {
Loop:
	for {
		select {
		case <-rc.snapshotC:
			rc.doSnapshot()
		case <-ctx.Done():
			break Loop
		}
	}
}

func (rc *raftNode) processPropose(ctx context.Context) {
	confChangeCount := uint64(0)

Loop:
	for {
		select {
		case prop, ok := <-rc.proposeC:
			if !ok {
				break Loop
			}

			// blocks until accepted by raft state machine
			// TODO:: handle dropped proposal
			err := rc.node.Propose(context.TODO(), []byte(prop))
			if err != nil {
				rc.logger.Warn("proposal fail", zap.String("err", err.Error()))
			}
		case cc, ok := <-rc.confChangeC:
			if !ok {
				break Loop
			}

			confChangeCount++
			cc.ID = confChangeCount
			rc.node.ProposeConfChange(context.TODO(), cc)
		case <-ctx.Done():
			break Loop
		}
	}
}

func (rc *raftNode) processRaftEvent(ctx context.Context) {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		rc.logger.Panic("serve channel", zap.String("err", err.Error()))
	}
	atomic.StoreUint64(&rc.snapshotIndex, snap.Metadata.Index)
	rc.appliedIndex = snap.Metadata.Index

	ticker := time.NewTicker(rc.raftTick)
	defer ticker.Stop()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			rc.membership.updateState(rd.SoftState)
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
				rc.recoverMembership(rd.Snapshot)
			}

			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rc.processMessages(rd.Messages))
			if ok := rc.publishEntries(ctx, rc.entriesToApply(rd.CommittedEntries)); ok {
				rc.maybeTriggerSnapshot()
				rc.node.Advance()
			}

		case err := <-rc.transport.ErrorC:
			rc.logger.Panic("transport error", zap.String("err", err.Error()))

		case <-ctx.Done():
			rc.stopRaft()
			return
		}
	}
}

func (rc *raftNode) runRaft(ctx context.Context) {
	url, err := url.Parse(rc.url)
	if err != nil {
		rc.logger.Panic("Failed parsing URL", zap.String("err", err.Error()))
	}

	ln, err := netutil.NewStoppableListener(ctx, url.Host)
	if err != nil {
		rc.logger.Panic("Failed to listen rafthttp", zap.String("err", err.Error()))
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-ctx.Done():
	default:
		rc.logger.Panic("Failed to serve rafthttp", zap.String("err", err.Error()))
	}
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

func (rc *raftNode) IsIDRemoved(id uint64) bool {
	return false
}

func (rc *raftNode) ReportUnreachable(id uint64) {
	rc.node.ReportUnreachable(id)
}

func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}

func (rc *raftNode) GetNodeID() varlogtypes.NodeID { return rc.id }
func (rc *raftNode) GetMembership() []string {
	return rc.membership.getMemberUrls()
}

func newRaftMemebership() *raftMembership {
	return &raftMembership{
		peers:   make(map[varlogtypes.NodeID]string),
		members: make(map[varlogtypes.NodeID]string),
	}
}

func (rc *raftNode) recoverMembership(snapshot raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshot) {
		return
	}

	stateMachine := &pb.MetadataRepositoryDescriptor{}
	err := stateMachine.Unmarshal(snapshot.Data)
	if err != nil {
		rc.logger.Panic("invalid snapshot",
			zap.String("err", err.Error()),
		)
	}

	rc.transport.RemoveAllPeers()
	rc.membership.removeAllPeers()

	for nodeID, peer := range stateMachine.Peers {
		if nodeID == rc.id {
			rc.url = peer
		} else {
			rc.transport.AddPeer(types.ID(nodeID), []string{peer})
		}
		rc.membership.addMember(nodeID, peer)
	}

	return
}

func (rm *raftMembership) addMember(nodeID varlogtypes.NodeID, url string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, ok := rm.members[nodeID]; ok {
		return
	}

	rm.peers[nodeID] = url
	rm.members[nodeID] = url
}

func (rm *raftMembership) addPeer(nodeID varlogtypes.NodeID, url string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, ok := rm.peers[nodeID]; ok {
		return
	}

	rm.peers[nodeID] = url
}

func (rm *raftMembership) removePeer(nodeID varlogtypes.NodeID) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	_, ok := rm.peers[nodeID]
	if !ok {
		return false
	}

	delete(rm.peers, nodeID)
	delete(rm.members, nodeID)
	return true
}

func (rm *raftMembership) getMemberUrls() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	m := make([]string, 0, len(rm.members))
	for _, url := range rm.members {
		m = append(m, url)
	}

	return m
}

func (rm *raftMembership) getMembers() []types.ID {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	m := make([]types.ID, 0, len(rm.members))
	for nodeID, _ := range rm.members {
		m = append(m, types.ID(nodeID))
	}

	return m
}

func (rm *raftMembership) removeAllPeers() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.peers = make(map[varlogtypes.NodeID]string)
	rm.members = make(map[varlogtypes.NodeID]string)
}

func (rm *raftMembership) isMember(nodeID varlogtypes.NodeID) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	_, ok := rm.members[nodeID]
	return ok
}

func (rm *raftMembership) getPeer(nodeID varlogtypes.NodeID) []byte {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	peer, ok := rm.peers[nodeID]
	if !ok {
		return nil
	}

	return []byte(peer)
}

func (rm *raftMembership) updateState(state *raft.SoftState) {
	if state == nil {
		return
	}

	if state.Lead != raft.None {
		atomic.StoreUint64((*uint64)(&rm.leader), uint64(state.Lead))
	}

	atomic.StoreUint64((*uint64)(&rm.state), uint64(state.RaftState))
}

func (rm *raftMembership) isLeader() bool {
	return raft.StateLeader == raft.StateType(atomic.LoadUint64((*uint64)(&rm.state)))
}

func (rm *raftMembership) hasLeader() bool {
	return rm.getLeader() != raft.None
}

func (rm *raftMembership) getLeader() uint64 {
	return atomic.LoadUint64((*uint64)(&rm.leader))
}

func (rm *raftMembership) clearMembership() {
	atomic.StoreUint64((*uint64)(&rm.leader), raft.None)
	atomic.StoreUint64((*uint64)(&rm.state), uint64(raft.StateFollower))
	rm.removeAllPeers()
}
