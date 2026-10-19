package core

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestBridgeHealthListenerFailureIsNotIdle(t *testing.T) {
	occupied, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	port := occupied.Addr().(*net.TCPAddr).Port
	bs := NewBridgeServer(port, "synthetic-test-token", "/bridge/ws", nil)
	bp := bs.NewPlatform("test-proj")
	e := NewEngine("test-proj", &stubAgent{}, []Platform{bp}, "", LangEnglish)
	bs.RegisterEngine("test-proj", e, bp)
	t.Cleanup(bs.Stop)
	bs.Start()

	assertListenerFailure := func() {
		t.Helper()
		status := e.PlatformStatuses()[0]
		var listenerError *net.OpError
		if status.Healthy() || status.Idle() || status.Usable() || !errors.As(status.Err, &listenerError) {
			t.Fatalf("failed listener reported healthy or lost its error: %+v", status)
		}
	}
	assertListenerFailure()
	// Neither adapter-ready nor last-disconnect transitions may overwrite a
	// listener failure with nil or the harmless idle sentinel.
	bs.setEnginePlatformsReady()
	assertListenerFailure()
	bs.setEnginePlatformsUnavailable()
	assertListenerFailure()
}

func TestBridgeHealthIdleReconnectPreservesMessageRouting(t *testing.T) {
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reservation.Addr().(*net.TCPAddr).Port
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	bs := NewBridgeServer(port, "synthetic-test-token", "/bridge/ws", nil)
	bp := bs.NewPlatform("test-proj")
	e := NewEngine("test-proj", &stubAgent{}, []Platform{bp}, "", LangEnglish)
	bs.RegisterEngine("test-proj", e, bp)
	t.Cleanup(bs.Stop)

	type exchange struct {
		message *Message
		err     error
	}
	received := make(chan exchange, 2)
	bp.handler = func(p Platform, msg *Message) {
		err := p.Reply(context.Background(), msg.ReplyCtx, "echo: "+msg.Content)
		received <- exchange{message: msg, err: err}
	}
	bs.Start()
	assertIdle := func() {
		t.Helper()
		status := e.PlatformStatuses()[0]
		if !status.Idle() || !status.Healthy() || status.Usable() || !errors.Is(status.Err, ErrPlatformIdle) {
			t.Fatalf("listener with no adapters should be healthy idle, not usable: %+v", status)
		}
		manifest := e.QueryAgentCapabilityManifest("", "", false)
		adapter := findRuntimeAdapter(t, manifest.Runtime, "platform", "bridge")
		if findRuntimeFeature(t, adapter.Capabilities, "text").Availability.State == CapabilityAvailable {
			t.Fatal("idle Bridge advertised text delivery as available")
		}
	}
	assertIdle()
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/bridge/ws", port)
	for round := 0; round < 2; round++ {
		headers := http.Header{"Authorization": []string{"Bearer synthetic-test-token"}}
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		register(t, conn, "web", []string{"text"})
		waitForBridgeStatus(t, e, func(status PlatformStatus) bool {
			return status.Usable() && status.Healthy() && !status.Idle()
		})
		content := fmt.Sprintf("message-%d", round)
		mustWriteJSON(t, conn, map[string]any{
			"type": "message", "msg_id": fmt.Sprintf("id-%d", round),
			"session_key": "bridge:web-admin:test-proj", "project": "test-proj",
			"user_id": "synthetic-user", "user_name": "Synthetic", "content": content,
			"reply_ctx": "synthetic-context",
		})
		reply := readMsg(t, conn)
		if reply["type"] != "reply" || reply["content"] != "echo: "+content || reply["reply_ctx"] != "synthetic-context" {
			t.Fatalf("round %d reply routing failed: %+v", round, reply)
		}
		var got exchange
		select {
		case got = <-received:
		case <-time.After(3 * time.Second):
			t.Fatal("message was not routed to the registered platform")
		}
		if got.err != nil || got.message.Content != content || got.message.UserID != "synthetic-user" || got.message.Platform != "web" {
			t.Fatalf("round %d inbound identity/content or reply failed: %+v", round, got)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		waitForBridgeStatus(t, e, func(status PlatformStatus) bool { return status.Idle() })
		assertIdle()
		if err := bp.Reply(context.Background(), got.message.ReplyCtx, "offline"); err == nil {
			t.Fatal("idle Bridge reported successful delivery without an adapter")
		}
	}
}
