package outboundgroup

import (
	"errors"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
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
