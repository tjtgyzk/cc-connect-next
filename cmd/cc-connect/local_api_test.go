package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/timmyagentic/cc-connect-next/core"
)

func TestReadRuntimeHealthDecodesReadyUnavailableAndIdleResponses(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix socket fixture")
	}
	for _, tc := range []struct {
		name   string
		health core.RuntimeHealth
	}{
		{name: "starting", health: core.RuntimeHealth{}},
		{name: "ready", health: core.RuntimeHealth{Ready: true}},
		{name: "idle", health: core.RuntimeHealth{Ready: true, Projects: []core.RuntimeProjectHealth{{
			Name: "alpha", Ready: true, Platforms: []core.RuntimePlatformHealth{{Name: "bridge", State: core.RuntimePlatformIdle, Reason: "waiting for adapter connections"}},
		}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "ccn-health-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			socketPath := filepath.Join(dir, "api.sock")
			listener, err := net.Listen("unix", socketPath)
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if !tc.health.Ready {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
				_ = json.NewEncoder(w).Encode(tc.health)
			})}
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() { _ = server.Close() })

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			health, err := readRuntimeHealth(ctx, socketPath)
			if err != nil || health.Ready != tc.health.Ready {
				t.Fatalf("health = %+v err=%v", health, err)
			}
			if tc.name == "idle" {
				state, lines := summarizeRuntimeHealth(health)
				if state != "Ready" || len(lines) != 1 || lines[0] != "alpha/bridge: idle (waiting for adapter connections)" {
					t.Fatalf("idle HTTP health lost its readiness or explanation: %q / %v", state, lines)
				}
			}
		})
	}
}

func TestSummarizeRuntimeHealth_IdleDoesNotMaskOtherPlatformStates(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ready      bool
		otherState core.RuntimePlatformState
		wantState  string
	}{
		{name: "ready and idle", ready: true, otherState: core.RuntimePlatformReady, wantState: "Ready"},
		{name: "starting and idle", otherState: core.RuntimePlatformStarting, wantState: "Starting"},
		{name: "unavailable and idle", otherState: core.RuntimePlatformUnavailable, wantState: "Unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			health := core.RuntimeHealth{Ready: tc.ready, Projects: []core.RuntimeProjectHealth{{
				Name: "alpha", Platforms: []core.RuntimePlatformHealth{
					{Name: "feishu", State: tc.otherState},
					{Name: "bridge", State: core.RuntimePlatformIdle},
				},
			}}}
			state, lines := summarizeRuntimeHealth(health)
			if state != tc.wantState || len(lines) != 2 || lines[0] != "alpha/bridge: idle" || lines[1] != "alpha/feishu: "+string(tc.otherState) {
				t.Fatalf("summary = %q / %v, want %q and per-platform states", state, lines, tc.wantState)
			}
		})
	}
}

func TestSummarizeRuntimeHealth(t *testing.T) {
	health := core.RuntimeHealth{Projects: []core.RuntimeProjectHealth{{
		Name: "alpha",
		Platforms: []core.RuntimePlatformHealth{
			{Name: "feishu", State: core.RuntimePlatformReady},
			{Name: "telegram", State: core.RuntimePlatformUnavailable, Reason: "connection refused"},
		},
	}}}
	state, lines := summarizeRuntimeHealth(health)
	if state != "Unavailable" || len(lines) != 2 || !strings.Contains(lines[1], "alpha/telegram") {
		t.Fatalf("summary = %q / %v", state, lines)
	}
}

func TestRuntimeAPIReachableDistinguishesMissingAndListeningSockets(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix socket fixture")
	}
	dir, err := os.MkdirTemp("/tmp", "ccn-reachable-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socketPath := filepath.Join(dir, "api.sock")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if reachable, err := runtimeAPIReachable(ctx, socketPath); err != nil || reachable {
		t.Fatalf("missing socket reachable=%t err=%v", reachable, err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	if reachable, err := runtimeAPIReachable(ctx, socketPath); err != nil || !reachable {
		t.Fatalf("listening socket reachable=%t err=%v", reachable, err)
	}
}
