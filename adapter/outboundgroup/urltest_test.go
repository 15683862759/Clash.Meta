package outboundgroup

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type healthCheckProvider struct {
	healthCheckDelay time.Duration
	proxies          []C.Proxy
}

func (p *healthCheckProvider) Name() string {
	return "provider"
}

func (p *healthCheckProvider) VehicleType() P.VehicleType {
	return P.Compatible
}

func (p *healthCheckProvider) Type() P.ProviderType {
	return P.Proxy
}

func (p *healthCheckProvider) Initial() error {
	return nil
}

func (p *healthCheckProvider) Update() error {
	return nil
}

func (p *healthCheckProvider) Proxies() []C.Proxy {
	return p.proxies
}

func (p *healthCheckProvider) Count() int {
	return len(p.proxies)
}

func (p *healthCheckProvider) Touch() {}

func (p *healthCheckProvider) HealthCheck() {
	time.Sleep(p.healthCheckDelay)
}

func (p *healthCheckProvider) Version() uint32 {
	return 1
}

func (p *healthCheckProvider) RegisterHealthCheckTask(
	string,
	utils.IntRanges[uint16],
	string,
	uint,
) {
}

func (p *healthCheckProvider) HealthCheckURL() string {
	return proxyHealthCheckURL
}

type testDelayProxy struct {
	*outbound.Base
	delay        uint16
	alive        bool
	urlTestDelay time.Duration
}

func newTestDelayProxy(name string, delay uint16, alive bool) *testDelayProxy {
	return &testDelayProxy{
		Base:  outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Direct}),
		delay: delay,
		alive: alive,
	}
}

func (p *testDelayProxy) Adapter() C.ProxyAdapter {
	return p
}

func (p *testDelayProxy) AliveForTestUrl(string) bool {
	return p.alive
}

func (p *testDelayProxy) DelayHistory() []C.DelayHistory {
	return []C.DelayHistory{{Time: time.Now(), Delay: p.delay}}
}

func (p *testDelayProxy) ExtraDelayHistories() map[string]C.ProxyState {
	return map[string]C.ProxyState{
		proxyHealthCheckURL: {Alive: p.alive, History: p.DelayHistory()},
	}
}

func (p *testDelayProxy) LastDelayForTestUrl(string) uint16 {
	if !p.alive {
		return ^uint16(0)
	}
	return p.delay
}

func (p *testDelayProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	time.Sleep(p.urlTestDelay)
	return p.delay, nil
}

func TestURLTestSwitchesToCompletedFastNodeBeforeCheckFinishes(t *testing.T) {
	originalInterval := fastRecheckInterval
	fastRecheckInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		fastRecheckInterval = originalInterval
	})

	slowProxy := newTestDelayProxy("slow", 300, true)
	fastProxy := newTestDelayProxy("fast", 50, false)
	provider := &healthCheckProvider{
		healthCheckDelay: 35 * time.Millisecond,
		proxies:          []C.Proxy{slowProxy, fastProxy},
	}
	group := NewURLTest(
		&GroupCommonOption{Name: "auto", URL: proxyHealthCheckURL},
		[]P.ProxyProvider{provider},
	)

	if got := group.Now(); got != "slow" {
		t.Fatalf("expected the first available node to be selected, got %q", got)
	}

	fastProxy.alive = true
	done := make(chan struct{})
	go func() {
		group.healthCheck()
		close(done)
	}()

	deadline := time.After(200 * time.Millisecond)
	for {
		if got := group.Now(); got == "fast" {
			break
		}
		select {
		case <-done:
			t.Fatal("expected to switch before the whole health check finished")
		case <-deadline:
			t.Fatalf("expected completed fast node to be selected, got %q", group.Now())
		case <-time.After(time.Millisecond):
		}
	}

	<-done
}

func TestManualURLTestSwitchesToCompletedFastNodeBeforeFinishes(t *testing.T) {
	originalInterval := fastRecheckInterval
	fastRecheckInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		fastRecheckInterval = originalInterval
	})

	slowProxy := newTestDelayProxy("slow", 300, true)
	fastProxy := newTestDelayProxy("fast", 50, false)
	fastProxy.urlTestDelay = 50 * time.Millisecond
	slowProxy.urlTestDelay = 250 * time.Millisecond
	provider := &healthCheckProvider{
		proxies: []C.Proxy{slowProxy, fastProxy},
	}
	group := NewURLTest(
		&GroupCommonOption{Name: "auto", URL: proxyHealthCheckURL},
		[]P.ProxyProvider{provider},
	)

	if got := group.Now(); got != "slow" {
		t.Fatalf("expected the first available node to be selected, got %q", got)
	}

	fastProxy.alive = true
	done := make(chan struct{})
	go func() {
		_, err := group.URLTest(context.Background(), proxyHealthCheckURL, nil)
		if err != nil {
			t.Errorf("URLTest returned error: %v", err)
		}
		close(done)
	}()

	deadline := time.After(400 * time.Millisecond)
	for {
		if got := group.Now(); got == "fast" {
			break
		}
		select {
		case <-done:
			t.Fatal("expected to switch before the manual URL test finished")
		case <-deadline:
			t.Fatalf("expected completed fast node to be selected during manual URL test, got %q", group.Now())
		case <-time.After(time.Millisecond):
		}
	}

	<-done
}

const proxyHealthCheckURL = "https://example.com/generate_204"
