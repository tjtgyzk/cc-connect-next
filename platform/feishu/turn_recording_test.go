package feishu

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/timmyagentic/cc-connect-next/core"
)

func TestParseTurnRecordOptions(t *testing.T) {
	// defaults
	cfg, err := parseTurnRecordOptions(map[string]any{})
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if cfg.enabled {
		t.Fatalf("expected disabled by default")
	}
	if cfg.wikiToken != defaultTurnRecordWikiToken || cfg.tableID != defaultTurnRecordTableID {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	if cfg.fields.Time != "时间" || cfg.fields.UserID != "user_id" || cfg.fields.Answer != "答案" {
		t.Fatalf("default fields wrong: %+v", cfg.fields)
	}

	// enabled with overrides
	cfg, err = parseTurnRecordOptions(map[string]any{
		"turn_record_enabled":   true,
		"turn_record_app_token": "bascn_app",
		"turn_record_table_id":  "tblX",
		"turn_record_fields":    map[string]any{"name": "昵称A", "question": "Q"},
	})
	if err != nil {
		t.Fatalf("overrides: %v", err)
	}
	if !cfg.enabled || cfg.appToken != "bascn_app" || cfg.tableID != "tblX" {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.fields.Name != "昵称A" || cfg.fields.Question != "Q" || cfg.fields.Answer != "答案" {
		t.Fatalf("field overrides wrong: %+v", cfg.fields)
	}

	// type errors
	if _, err := parseTurnRecordOptions(map[string]any{"turn_record_enabled": "yes"}); err == nil {
		t.Fatalf("expected error for non-boolean enabled")
	}
	if _, err := parseTurnRecordOptions(map[string]any{"turn_record_fields": "nope"}); err == nil {
		t.Fatalf("expected error for non-table fields")
	}
}

type turnRecordServer struct {
	mu         sync.Mutex
	wikiCalls  int
	recordReq  []map[string]any
	recordPath []string
}

func (s *turnRecordServer) handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	respond := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	switch {
	case strings.Contains(r.URL.Path, "/auth/v3/tenant_access_token/internal"):
		respond(map[string]any{"code": 0, "msg": "ok", "tenant_access_token": "t-token", "expire": 7200})
	case strings.Contains(r.URL.Path, "/wiki/v2/spaces/get_node"):
		s.mu.Lock()
		s.wikiCalls++
		s.mu.Unlock()
		respond(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{"node": map[string]any{"obj_token": "bascnResolved", "obj_type": "bitable"}}})
	case strings.Contains(r.URL.Path, "/contact/v3/users/"):
		respond(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{"user": map[string]any{"user_id": "u_123", "name": "Alice"}}})
	case strings.Contains(r.URL.Path, "/bitable/v1/apps/") && strings.HasSuffix(r.URL.Path, "/records"):
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.recordReq = append(s.recordReq, body)
		s.recordPath = append(s.recordPath, r.URL.Path)
		s.mu.Unlock()
		respond(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{"record": map[string]any{"record_id": "rec1"}}})
	default:
		respond(map[string]any{"code": 0, "msg": "ok"})
	}
}

func newTurnRecordFixture(t *testing.T) (*Platform, *turnRecordServer) {
	t.Helper()
	srv := &turnRecordServer{}
	server := httptest.NewServer(http.HandlerFunc(srv.handler))
	t.Cleanup(server.Close)
	raw, err := New(map[string]any{
		"app_id":              "cli_test",
		"app_secret":          "test",
		"domain":              server.URL,
		"enable_feishu_card":  false,
		"turn_record_enabled": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	p := raw.(*Platform)
	t.Cleanup(func() { _ = p.Stop() })
	return p, srv
}

func TestTurnRecorderWrite_ResolvesWikiAndCreatesRecord(t *testing.T) {
	p, srv := newTurnRecordFixture(t)
	rec := core.TurnRecord{
		At:         time.Date(2026, 10, 19, 10, 30, 0, 0, time.Local),
		UserID:     "ou_user",
		UserName:   "Alice",
		ChatName:   "Test Group",
		Model:      "deepseek-v4.1-flash",
		SessionKey: "feishu:oc_x:ou_user",
		Question:   "how do I do X?",
		Answer:     "do Y then Z",
		MessageID:  "om_1",
	}

	p.turnRecording.write(p, rec)
	p.turnRecording.write(p, rec) // second write must reuse the cached app token

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.wikiCalls != 1 {
		t.Fatalf("wiki resolve calls = %d, want 1 (cached)", srv.wikiCalls)
	}
	if len(srv.recordReq) != 2 {
		t.Fatalf("record requests = %d, want 2", len(srv.recordReq))
	}
	if !strings.Contains(srv.recordPath[0], "/apps/bascnResolved/tables/"+defaultTurnRecordTableID+"/records") {
		t.Fatalf("unexpected record path: %s", srv.recordPath[0])
	}
	body := srv.recordReq[0]
	fields, _ := body["fields"].(map[string]any)
	if fields == nil {
		t.Fatalf("missing fields: %+v", body)
	}
	if fields["时间"] != "2026-10-19 10:30:00" {
		t.Fatalf("time field = %v", fields["时间"])
	}
	if fields["昵称"] != "Alice" || fields["user_id"] != "u_123" {
		t.Fatalf("identity fields wrong: %+v", fields)
	}
	if fields["群名"] != "Test Group" || fields["模型"] != "deepseek-v4.1-flash" {
		t.Fatalf("context fields wrong: %+v", fields)
	}
	if fields["问题"] != "how do I do X?" || fields["答案"] != "do Y then Z" {
		t.Fatalf("content fields wrong: %+v", fields)
	}
	// idempotency key present and stable across the two identical writes
	if body["client_token"] == "" || body["client_token"] != srv.recordReq[1]["client_token"] {
		t.Fatalf("client_token not stable: %v vs %v", body["client_token"], srv.recordReq[1]["client_token"])
	}
}

func TestTurnRecorder_RecordTurnEnqueuesAndStops(t *testing.T) {
	p, _ := newTurnRecordFixture(t)
	if !p.TurnRecordingEnabled() {
		t.Fatalf("TurnRecordingEnabled = false, want true")
	}
	if err := p.RecordTurn(context.Background(), core.TurnRecord{MessageID: "om_2", Answer: "a"}); err != nil {
		t.Fatalf("RecordTurn: %v", err)
	}
	// Disabled platform is a no-op.
	raw, _ := New(map[string]any{"app_id": "cli_x", "app_secret": "s", "enable_feishu_card": false})
	disabled := raw.(*Platform)
	if disabled.TurnRecordingEnabled() {
		t.Fatalf("disabled platform reports enabled")
	}
	if err := disabled.RecordTurn(context.Background(), core.TurnRecord{}); err != nil {
		t.Fatalf("disabled RecordTurn should be no-op: %v", err)
	}
}

func TestTurnRecorder_AvailableThroughInteractiveCardWrapper(t *testing.T) {
	// Default config wraps *Platform in interactivePlatform; the promoted
	// TurnRecorder methods must still satisfy the core capability assertion.
	srv := &turnRecordServer{}
	server := httptest.NewServer(http.HandlerFunc(srv.handler))
	t.Cleanup(server.Close)
	raw, err := New(map[string]any{
		"app_id":              "cli_wrap",
		"app_secret":          "s",
		"domain":              server.URL,
		"enable_feishu_card":  true,
		"turn_record_enabled": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Stop() })
	if _, ok := raw.(*interactivePlatform); !ok {
		t.Fatalf("expected interactivePlatform wrapper, got %T", raw)
	}
	recorder, ok := raw.(core.TurnRecorder)
	if !ok {
		t.Fatalf("wrapped platform does not satisfy core.TurnRecorder")
	}
	if !recorder.TurnRecordingEnabled() {
		t.Fatalf("wrapped platform reports recording disabled")
	}
}

func TestResolveUserInfo_CachesUserIDAndName(t *testing.T) {
	p, _ := newTurnRecordFixture(t)
	userID, name := p.resolveUserInfo("ou_user")
	if userID != "u_123" || name != "Alice" {
		t.Fatalf("resolveUserInfo = %q,%q", userID, name)
	}
	// cached second call
	userID2, name2 := p.resolveUserInfo("ou_user")
	if userID2 != "u_123" || name2 != "Alice" {
		t.Fatalf("cached resolveUserInfo = %q,%q", userID2, name2)
	}
}
