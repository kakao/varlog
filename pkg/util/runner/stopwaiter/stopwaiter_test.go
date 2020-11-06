package stopwaiter

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestStopWaiter(t *testing.T) {
	sw := New()
	if sw.Stopped() {
		t.Error("stopped: expected=false actual=true")
	}
	done := make(chan struct{})
	go func() {
		<-done
		sw.Stop()
	}()
	if sw.Stopped() {
		t.Error("stopped: expected=false actual=true")
	}
	close(done)
	sw.Wait()
	if !sw.Stopped() {
		t.Error("stopped: expected=true actual=false")
	}

	var called int32

	var wg sync.WaitGroup
	wg.Add(1)
	onStop := func() {
		atomic.AddInt32(&called, 1)
		wg.Done()
	}
	sw = NewWithOnStop(onStop)
	go func() {
		sw.Stop()
	}()
	go func() {
		sw.Stop()
	}()
	go func() {
		sw.Stop()
	}()
	wg.Wait()
	if !sw.Stopped() {
		t.Error("stopped: expected=true actual=false")
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Errorf("called: expected=1 actual=%v", atomic.LoadInt32(&called))
	}
}
