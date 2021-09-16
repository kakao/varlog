package stopchannel

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestStopChannel(t *testing.T) {
	sc := New()
	sc.Stop()
	assert.True(t, sc.Stopped())
	_, ok := <-sc.StopC()
	assert.False(t, ok)
}

func TestStopChannelGoroutines(t *testing.T) {
	const concurrency = 100

	defer goleak.VerifyNone(t)

	sc := New()

	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			<-sc.StopC()
			wg.Done()
		}()
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		sc.Stop()
	}()
	wg.Wait()
}
