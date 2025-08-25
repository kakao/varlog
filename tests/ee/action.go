package ee

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
)

// Action tests basic APIs of the Varlog, which are Append, AppendTo,
// Subscribe, and SubscribeTo while doing reconfigurations of the cluster.
type Action struct {
	actionConfig
	appended struct {
		glsn types.GLSN
		mu   sync.Mutex
	}
	ids struct {
		idmap map[types.TopicID][]types.LogStreamID
		mu    sync.Mutex
	}
}

// NewAction creates a new Action.
// Users should define all reconfigurations to cluster by using ConfChanger.
func NewAction(t *testing.T, opts ...ActionOption) *Action {
	t.Helper()
	cfg := newActionConfig(t, opts)
	act := &Action{
		actionConfig: cfg,
	}
	act.ids.idmap = make(map[types.TopicID][]types.LogStreamID)
	return act
}

// AddTopic adds a new topic to the Action while running.
// It allows knowing topics added in runtime.
func (action *Action) AddTopic(tpid types.TopicID) {
	action.ids.mu.Lock()
	defer action.ids.mu.Unlock()
	action.logger.Info("adding a new topic",
		zap.Int32("tpid", int32(tpid)),
	)
	if _, ok := action.ids.idmap[tpid]; !ok {
		action.ids.idmap[tpid] = []types.LogStreamID{}
	}
}

// AddLogStream adds a new log stream to the Action while running.
// It allows knowing log streams added in runtime.
func (action *Action) AddLogStream(tpid types.TopicID, lsid types.LogStreamID) {
	action.ids.mu.Lock()
	defer action.ids.mu.Unlock()
	action.logger.Info("adding a new log stream to topic",
		zap.Int32("tpid", int32(tpid)),
		zap.Int32("lsid", int32(lsid)),
	)
	if _, ok := action.ids.idmap[tpid]; !ok {
		action.ids.idmap[tpid] = []types.LogStreamID{}
	}
	lsids := action.ids.idmap[tpid]
	lsids = append(lsids, lsid)
	action.ids.idmap[tpid] = lsids
}

// Do runs the action.
func (action *Action) Do(ctx context.Context, t *testing.T) {
	t.Helper()

	action.logger.Info("starting Action",
		zap.Int("numAppendClients", action.numAppendClients),
		zap.Int("numSubscribeClients", action.numSubscribeClients),
	)
	if action.preHook != nil {
		action.preHook(t, action)
	}

	// The quitClients stops appendClients and subscribeClients.
	quitClients := make(chan struct{})
	g, gctx := errgroup.WithContext(ctx)
	defer func() {
		close(quitClients)
		assert.NoError(t, g.Wait()) // no error clients
		if action.postHook != nil {
			action.postHook(t, action)
		}
		action.logger.Info("Action finished")
	}()

	for i := 0; i < action.numAppendClients; i++ {
		g.Go(func() error {
			return action.append(gctx, quitClients)
		})
	}
	// for i := 0; i < action.numSubscribeClients; i++ {
	//	g.Go(func() error {
	//		return action.subscribe(gctx, quitClients)
	//	})
	// }

	for i := 0; i < action.numRepeats; i++ {
		for _, cc := range action.confChangers {
			// ConfChanger will fail if one of the clients fails
			// since confChanger uses the same context with
			// clients.
			if !cc.Do(gctx, t) {
				return
			}
			action.logger.Info("conf changer finished",
				zap.String("name", cc.Name()),
				zap.Int("count", i+1),
			)
		}
	}
}

// func (act *Action) subscribe(ctx context.Context, done <-chan struct{}) error {
//	vcli, err := varlog.Open(ctx, act.testClusterID, []string{act.mrAddr},
//		varlog.WithDenyTTL(5*time.Second),
//		varlog.WithOpenTimeout(10*time.Second),
//	)
//
//	if err != nil {
//		return err
//	}
//	defer vcli.Close()
//
//	res := vcli.Append(ctx, act.topicID, [][]byte{[]byte("foo")}, varlog.WithRetryCount(5))
//	if res.Err != nil {
//		return errors.Wrap(res.Err, "append")
//	}
//	limit := res.Metadata[0].GLSN
//
//	var received atomic.Value
//	received.Store(types.InvalidGLSN)
//	serrC := make(chan error)
//	nopOnNext := func(le varlogpb.LogEntry, err error) {
//		if err != nil {
//			serrC <- err
//			close(serrC)
//		} else {
//			received.Store(le.GLSN)
//		}
//	}
//
//	fmt.Printf("[%v] Sub ~%v\n", time.Now(), limit)
//	defer fmt.Printf("[%v] Sub ~%v Close\n", time.Now(), limit)
//
//	closer, err := vcli.Subscribe(ctx, act.topicID, types.MinGLSN, limit+types.GLSN(1), nopOnNext)
//	if err != nil {
//		return err
//	}
//	defer closer()
//
//	subscribeCheckInterval := 3 * time.Second
//	timer := time.NewTimer(subscribeCheckInterval)
//	defer timer.Stop()
//
//	prevGLSN := types.InvalidGLSN
//Loop:
//	for {
//		select {
//		case err, alive := <-serrC:
//			if !alive {
//				break Loop
//			}
//
//			if err != io.EOF {
//				return err
//			}
//		case <-timer.C:
//			cur := received.Load().(types.GLSN)
//			fmt.Printf("[%v] Sub ~%v checkFunc prev:%v, cur:%v\n", time.Now(), limit, prevGLSN, cur)
//			if cur == prevGLSN {
//				return fmt.Errorf("subscribe timeout")
//			}
//			prevGLSN = cur
//
//			timer.Reset(subscribeCheckInterval)
//		}
//	}
//
//	return nil
//}

func (action *Action) append(ctx context.Context, quitClients <-chan struct{}) error {
	vcli, err := varlog.Open(ctx, action.clusterID, []string{action.mrAddr},
		varlog.WithDenyTTL(5*time.Second),
		varlog.WithOpenTimeout(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("Action: open client: %w", err)
	}
	defer func() {
		_ = vcli.Close()
	}()

	for {
		select {
		case <-quitClients:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("Action: append: %w", ctx.Err())
		default:
			tpid, err := action.getRandomTopidID()
			if err != nil {
				return fmt.Errorf("Action: append: %w", err)
			}
			res := vcli.Append(ctx, tpid, [][]byte{[]byte("foo")}, varlog.WithRetryCount(5))
			if res.Err != nil {
				return fmt.Errorf("Action: append: %w", res.Err)
			}
			action.setAppendResult(res.Metadata[0].GLSN)
		}
	}
}

func (action *Action) setAppendResult(glsn types.GLSN) {
	action.appended.mu.Lock()
	defer action.appended.mu.Unlock()

	if action.appended.glsn < glsn {
		action.appended.glsn = glsn
	}
}

func (action *Action) getRandomTopidID() (types.TopicID, error) {
	action.ids.mu.Lock()
	defer action.ids.mu.Unlock()

	tpids := make([]types.TopicID, 0, len(action.ids.idmap))
	for tpid, lsids := range action.ids.idmap {
		if len(lsids) > 0 {
			tpids = append(tpids, tpid)
		}
	}
	if len(tpids) == 0 {
		return 0, errors.New("Action: no available topic")
	}
	return tpids[rand.Intn(len(tpids))], nil
}
