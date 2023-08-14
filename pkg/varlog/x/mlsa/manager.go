package mlsa

import (
	"errors"

	"github.com/puzpuzpuz/xsync/v2"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
)

type managedLSA struct {
	mgr  *Manager
	lsa  varlog.LogStreamAppender
	tpid types.TopicID
	lsid types.LogStreamID
}

var _ varlog.LogStreamAppender = (*managedLSA)(nil)

func (m *managedLSA) AppendBatch(dataBatch [][]byte, callback varlog.BatchCallback) error {
	return m.lsa.AppendBatch(dataBatch, callback)
}

func (m *managedLSA) Close() {
	m.mgr.mu.Lock()
	appenders, ok := m.mgr.mlsas[m.tpid]
	if ok && appenders[m.lsid] == m {
		delete(appenders, m.lsid)
		m.mgr.count--
	}
	m.mgr.mu.Unlock()

	m.lsa.Close()
}

// Manager manages a set of LogStreamAppenders.
// When multiple users attempt to obtain the same LogStreamAppender using the
// same TopicID and LogStreamID, they may share the same object. Therefore,
// when one user calls the Close function, another may encounter an ErrClose
// error. To resolve this issue, users can simply get a new LogStreamAppender
// by calling the Get function again.
type Manager struct {
	mlsas map[types.TopicID]map[types.LogStreamID]*managedLSA
	count int
	mu    *xsync.RBMutex

	vcli varlog.Log
	opts []varlog.LogStreamAppenderOption
}

// New returns a new Manager. Generally, users only require one manager.
func New(vcli varlog.Log, opts ...varlog.LogStreamAppenderOption) *Manager {
	mgr := &Manager{
		vcli:  vcli,
		opts:  opts,
		mlsas: make(map[types.TopicID]map[types.LogStreamID]*managedLSA),
		mu:    xsync.NewRBMutex(),
	}
	return mgr
}

// Get returns a LogStreamAppender specified by the arguments tpid and lsid. It
// returns an error if it cannot access the given log stream.
func (mgr *Manager) Get(tpid types.TopicID, lsid types.LogStreamID) (varlog.LogStreamAppender, error) {
	t := mgr.mu.RLock()
	if lsa := mgr.mlsas[tpid][lsid]; lsa != nil {
		mgr.mu.RUnlock(t)
		return lsa, nil
	}
	mgr.mu.RUnlock(t)

	return mgr.getSlow(tpid, lsid)
}

func (mgr *Manager) getSlow(tpid types.TopicID, lsid types.LogStreamID) (varlog.LogStreamAppender, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	appenders, ok := mgr.mlsas[tpid]
	if !ok {
		appenders = make(map[types.LogStreamID]*managedLSA)
		mgr.mlsas[tpid] = appenders
	}

	mlsa, ok := appenders[lsid]
	if !ok {
		lsa, err := mgr.vcli.NewLogStreamAppender(tpid, lsid, mgr.opts...)
		if err != nil {
			return nil, err
		}
		mlsa = &managedLSA{
			lsa:  lsa,
			tpid: tpid,
			lsid: lsid,
			mgr:  mgr,
		}
		appenders[lsid] = mlsa
		mgr.count++
	}
	return mlsa, nil
}

// Any returns the LogStreamAppender that is filtered by the argument
// allowlist. If the allowlist is empty, all log streams in the topic can be
// chosen.
// It chooses the writable LogStreamAppender as possible.
func (mgr *Manager) Any(tpid types.TopicID, allowlist map[types.LogStreamID]struct{}) (varlog.LogStreamAppender, error) {
	var shorter, longer map[types.LogStreamID]struct{}
	candidates := mgr.vcli.AppendableLogStreams(tpid)
	if len(allowlist) == 0 || len(candidates) < len(allowlist) {
		shorter = candidates
		longer = allowlist
	} else {
		shorter = allowlist
		longer = candidates
	}

	for lsid := range shorter {
		if len(longer) > 0 {
			if _, ok := longer[lsid]; !ok {
				continue
			}
		}

		lsa, err := mgr.Get(tpid, lsid)
		if err == nil {
			return lsa, nil
		}
	}
	return nil, errors.New("no appendable log stream")
}

// Clear closes all the managed LogStreamAppender, and clears them. Clients can
// continue to use this Manager after calling Clear.
//
// After using the Manager, clients should call Clear to release any associated
// resources.
func (mgr *Manager) Clear() {
	lsas := mgr.clear()
	for _, lsa := range lsas {
		lsa.Close()
	}
}

func (mgr *Manager) clear() []varlog.LogStreamAppender {
	mgr.mu.Lock()
	defer func() {
		mgr.count = 0
		mgr.mu.Unlock()
	}()

	if len(mgr.mlsas) == 0 {
		return nil
	}

	lsas := make([]varlog.LogStreamAppender, 0, mgr.count)
	for tpid, appenders := range mgr.mlsas {
		for lsid, mlsa := range appenders {
			lsas = append(lsas, mlsa.lsa)
			delete(appenders, lsid)
		}
		delete(mgr.mlsas, tpid)
	}
	return lsas
}
