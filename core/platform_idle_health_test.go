package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPlatformStatusIdleIsHealthyButCannotDeliver(t *testing.T) {
	for _, ready := range []bool{false, true} {
		idle := PlatformStatus{Name: "custom-inbound", Ready: ready, Err: fmt.Errorf("no clients: %w", ErrPlatformIdle)}
		if !idle.Idle() || !idle.Healthy() || idle.Usable() {
			t.Fatalf("idle status must be healthy without claiming delivery: %+v", idle)
		}
		failed := PlatformStatus{Name: "custom-inbound", Ready: ready, Err: errors.New("listener failed")}
		if failed.Idle() || failed.Healthy() || failed.Usable() {
			t.Fatalf("real transport failure must stay unhealthy: %+v", failed)
		}
	}
}

func TestHandleHealthIdleDoesNotMaskOtherPlatforms(t *testing.T) {
	for _, test := range []struct {
		name       string
		withOther  bool
		otherReady bool
		otherErr   error
		wantCode   int
	}{
		{name: "inbound-only", wantCode: http.StatusOK},
		{name: "with-ready-platform", withOther: true, otherReady: true, wantCode: http.StatusOK},
		{name: "with-starting-platform", withOther: true, wantCode: http.StatusServiceUnavailable},
		{name: "with-broken-platform", withOther: true, otherReady: true, otherErr: errors.New("connection refused"), wantCode: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			idle := &stubUnhealthyPlatform{stubPlatformEngine: stubPlatformEngine{n: "custom-inbound"}, err: fmt.Errorf("no clients: %w", ErrPlatformIdle)}
			engine := NewEngine("test", &stubAgent{}, []Platform{idle}, "", LangEnglish)
			if test.withOther {
				other := &stubUnhealthyPlatform{stubPlatformEngine: stubPlatformEngine{n: "required-platform"}, err: test.otherErr}
				engine.AddPlatform(other)
				if test.otherReady {
					engine.OnPlatformReady(other)
				}
			}
			api := &APIServer{engines: map[string]*Engine{"test": engine}}
			rec := httptest.NewRecorder()
			api.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			var health RuntimeHealth
			if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
				t.Fatal(err)
			}
			if rec.Code != test.wantCode || health.Ready != (test.wantCode == http.StatusOK) {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			platform := health.Projects[0].Platforms[0]
			if platform.State != RuntimePlatformIdle || platform.Reason == "" {
				t.Fatalf("idle must stay visible with its reason: %+v", platform)
			}
		})
	}
}
