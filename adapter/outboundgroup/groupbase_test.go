package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

func waitForFailureCheck(t *testing.T, triggered chan struct{}, timeout time.Duration) bool {
	t.Helper()
	select {
	case <-triggered:
		return true
	case <-time.After(timeout):
		return false
	}
}

func TestOneFailedDialTriggersHealthCheck(t *testing.T) {
	group := NewGroupBase(GroupBaseOption{
		Name:           "auto",
		Type:           C.URLTest,
		TestTimeout:    9000,
		MaxFailedTimes: 1,
	})
	triggered := make(chan struct{}, 1)

	group.onDialFailed(C.Shadowsocks, errors.New("dial failed"), func() {
		triggered <- struct{}{}
	})

	if !waitForFailureCheck(t, triggered, time.Second) {
		t.Fatal("expected the first failed dial to trigger a health check when max-failed-times is 1")
	}
}

func TestSecondFailedDialWithinWindowTriggersHealthCheck(t *testing.T) {
	group := NewGroupBase(GroupBaseOption{
		Name:           "auto",
		Type:           C.URLTest,
		TestTimeout:    9000,
		MaxFailedTimes: 2,
	})
	triggered := make(chan struct{}, 1)
	check := func() {
		triggered <- struct{}{}
	}

	group.onDialFailed(C.Shadowsocks, errors.New("first dial failed"), check)
	group.failedTestMux.Lock()
	group.failedTestMux.Unlock()
	if waitForFailureCheck(t, triggered, 10*time.Millisecond) {
		t.Fatal("expected the first failed dial to stay below max-failed-times")
	}

	group.onDialFailed(C.Shadowsocks, errors.New("second dial failed"), check)
	if !waitForFailureCheck(t, triggered, time.Second) {
		t.Fatal("expected the second failed dial inside the window to trigger a health check")
	}
}

type concurrencyTracker struct {
	active  atomic.Int32
	max     atomic.Int32
	entered atomic.Int32
	start   chan struct{}
	barrier *sync.Once
}

type concurrencyTrackingProxy struct {
	*outbound.Base
	tracker *concurrencyTracker
}

func newConcurrencyTrackingProxy(name string, tracker *concurrencyTracker) *concurrencyTrackingProxy {
	return &concurrencyTrackingProxy{
		Base:    outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Direct}),
		tracker: tracker,
	}
}

func (p *concurrencyTrackingProxy) Adapter() C.ProxyAdapter {
	return p
}

func (p *concurrencyTrackingProxy) AliveForTestUrl(string) bool {
	return true
}

func (p *concurrencyTrackingProxy) LastDelayForTestUrl(string) uint16 {
	return 100
}

func (p *concurrencyTrackingProxy) DelayHistory() []C.DelayHistory {
	return []C.DelayHistory{}
}

func (p *concurrencyTrackingProxy) ExtraDelayHistories() map[string]C.ProxyState {
	return map[string]C.ProxyState{}
}

func (p *concurrencyTrackingProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	tracker := p.tracker
	tracker.entered.Add(1)
	active := tracker.active.Add(1)
	for {
		max := tracker.max.Load()
		if active <= max || tracker.max.CompareAndSwap(max, active) {
			break
		}
	}
	if active >= 15 {
		tracker.barrier.Do(func() { close(tracker.start) })
	}
	select {
	case <-tracker.start:
	case <-time.After(100 * time.Millisecond):
	}
	time.Sleep(20 * time.Millisecond)
	tracker.active.Add(-1)
	return 100, nil
}

func TestGroupURLTestLimitsConcurrentChecks(t *testing.T) {
	const limit = 10
	proxies := make([]C.Proxy, 0, 25)
	tracker := &concurrencyTracker{start: make(chan struct{}), barrier: &sync.Once{}}
	for i := 0; i < 25; i++ {
		proxies = append(proxies, newConcurrencyTrackingProxy(fmt.Sprintf("proxy-%02d", i), tracker))
	}
	provider := &healthCheckProvider{proxies: proxies}
	group := NewGroupBase(GroupBaseOption{
		Name:      "auto",
		Type:      C.URLTest,
		Providers: []P.ProxyProvider{provider},
	})

	results, err := group.URLTest(context.Background(), proxyHealthCheckURL, nil)
	if err != nil {
		t.Fatalf("URLTest returned error: %v", err)
	}
	if len(results) != len(proxies) {
		t.Fatalf("expected %d results, got %d", len(proxies), len(results))
	}
	if entered := tracker.entered.Load(); entered != int32(len(proxies)) {
		t.Fatalf("expected %d checks to start, got %d", len(proxies), entered)
	}
	highest := tracker.max.Load()
	t.Logf("entered %d checks; highest concurrency: %d", tracker.entered.Load(), highest)
	if highest > limit {
		t.Fatalf("expected at most %d concurrent delay checks, got %d", limit, highest)
	}
}
