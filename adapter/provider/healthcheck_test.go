package provider

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

type healthCheckProxy struct {
	*outbound.Base
	started chan struct{}
	release chan struct{}
}

func newHealthCheckProxy(name string) *healthCheckProxy {
	return &healthCheckProxy{
		Base:    outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Direct}),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (p *healthCheckProxy) Adapter() C.ProxyAdapter {
	return p
}

func (p *healthCheckProxy) AliveForTestUrl(string) bool {
	return true
}

func (p *healthCheckProxy) DelayHistory() []C.DelayHistory {
	return nil
}

func (p *healthCheckProxy) ExtraDelayHistories() map[string]C.ProxyState {
	return nil
}

func (p *healthCheckProxy) LastDelayForTestUrl(string) uint16 {
	return 100
}

func (p *healthCheckProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	close(p.started)
	<-p.release
	return 100, nil
}

func TestHealthCheckSurvivesProxyUpdateDuringCheck(t *testing.T) {
	proxy := newHealthCheckProxy("old")
	hc := NewHealthCheck(
		[]C.Proxy{proxy},
		"https://example.com/generate_204",
		100,
		0,
		false,
		nil,
	)

	done := make(chan struct{})
	go func() {
		hc.check()
		close(done)
	}()

	select {
	case <-proxy.started:
	case <-time.After(time.Second):
		t.Fatal("health check did not start")
	}

	hc.setProxies([]C.Proxy{proxy, newHealthCheckProxy("new")})
	close(proxy.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("health check did not finish")
	}
}
