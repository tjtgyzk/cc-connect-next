package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
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
	if cfg.fields.Requester != "" || cfg.fields.Trace != "" || cfg.fields.Duration != "" || cfg.fields.ToolCount != "" || cfg.fields.Summary != "" {
		t.Fatalf("enrichment fields must be opt-in: %+v", cfg.fields)
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
	mu           sync.Mutex
	wikiCalls    int
	contactCode  int
	recordReq    []map[string]any
	recordPath   []string
	clientTokens []string
	userIDTypes  []string
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
		s.mu.Lock()
		contactCode := s.contactCode
		s.mu.Unlock()
		if contactCode != 0 {
			respond(map[string]any{"code": contactCode, "msg": "synthetic lookup denied"})
			return
		}
		respond(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{"user": map[string]any{"user_id": "u_123", "name": "Alice"}}})
	case strings.Contains(r.URL.Path, "/bitable/v1/apps/") && strings.HasSuffix(r.URL.Path, "/records"):
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.recordReq = append(s.recordReq, body)
		s.recordPath = append(s.recordPath, r.URL.Path)
		s.clientTokens = append(s.clientTokens, r.URL.Query().Get("client_token"))
		s.userIDTypes = append(s.userIDTypes, r.URL.Query().Get("user_id_type"))
		s.mu.Unlock()
		respond(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{"record": map[string]any{"record_id": "rec1"}}})
	default:
		respond(map[string]any{"code": 0, "msg": "ok"})
	}
}

func newTurnRecordFixture(t *testing.T, extraOptions ...map[string]any) (*Platform, *turnRecordServer) {
	t.Helper()
	srv := &turnRecordServer{}
	server := httptest.NewServer(http.HandlerFunc(srv.handler))
	t.Cleanup(server.Close)
	opts := map[string]any{
		"app_id":              "cli_test",
		"app_secret":          "test",
		"domain":              server.URL,
		"enable_feishu_card":  false,
		"turn_record_enabled": true,
	}
	for _, extra := range extraOptions {
		for key, value := range extra {
			opts[key] = value
		}
	}
	raw, err := New(opts)
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
	if len(fields) != 7 {
		t.Fatalf("legacy configuration must only write its original seven columns: %+v", fields)
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
	// idempotency key is sent as the client_token query parameter, must be a
	// canonical UUID, and must be stable across the two identical writes.
	uuidRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	if len(srv.clientTokens) != 2 {
		t.Fatalf("client_token captures = %d, want 2", len(srv.clientTokens))
	}
	if !uuidRe.MatchString(srv.clientTokens[0]) {
		t.Fatalf("client_token not UUID-shaped: %q", srv.clientTokens[0])
	}
	// Feishu requires a proper UUID v4: version nibble must be 4, variant must be 8/9/a/b.
	ct := srv.clientTokens[0]
	if ct[14] != '4' {
		t.Fatalf("client_token version nibble = %q, want '4': %q", ct[14], ct)
	}
	if !strings.ContainsRune("89ab", rune(ct[19])) {
		t.Fatalf("client_token variant nibble = %q, want 8/9/a/b: %q", ct[19], ct)
	}
	if srv.clientTokens[0] != srv.clientTokens[1] {
		t.Fatalf("client_token not stable: %q vs %q", srv.clientTokens[0], srv.clientTokens[1])
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
	userID, name := p.resolveUserInfo(context.Background(), "ou_user")
	if userID != "u_123" || name != "Alice" {
		t.Fatalf("resolveUserInfo = %q,%q", userID, name)
	}
	// cached second call
	userID2, name2 := p.resolveUserInfo(context.Background(), "ou_user")
	if userID2 != "u_123" || name2 != "Alice" {
		t.Fatalf("cached resolveUserInfo = %q,%q", userID2, name2)
	}
}

type blockedTurnRecordContactTransport struct {
	base      http.RoundTripper
	entered   chan struct{}
	cancelled chan error
	release   chan struct{}
	once      sync.Once
}

func (b *blockedTurnRecordContactTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.Contains(req.URL.Path, "/contact/v3/users/") {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-req.Context().Done():
			b.cancelled <- req.Context().Err()
			return nil, req.Context().Err()
		case <-b.release:
			return nil, errors.New("contact fixture released")
		}
	}
	return b.base.RoundTrip(req)
}

func TestTurnRecorder_WriteContactLookupHonorsCancellationAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "recorder stopped", true: "parent deadline"}[deadline], func(t *testing.T) {
			srv := &turnRecordServer{}
			server := httptest.NewServer(http.HandlerFunc(srv.handler))
			transport := &blockedTurnRecordContactTransport{base: server.Client().Transport,
				entered: make(chan struct{}), cancelled: make(chan error, 1), release: make(chan struct{})}
			p := &Platform{platformName: "feishu", client: lark.NewClient("cli_contact_"+t.Name(), "test",
				lark.WithOpenBaseUrl(server.URL), lark.WithHttpClient(&http.Client{Transport: transport}))}
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			}
			r := &turnRecorder{cfg: &turnRecordConfig{enabled: true, appToken: "bascnTest", tableID: "tblTest", fields: defaultTurnRecordFields()}, ctx: ctx, cancel: cancel}
			done := make(chan struct{})
			defer func() {
				r.stop()
				close(transport.release)
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("write did not stop after fixture release")
				}
				server.Close()
			}()
			go func() {
				defer close(done)
				r.write(p, core.TurnRecord{UserID: "ou_test", MessageID: "om_test", Question: "question", Answer: "answer", At: time.Now()})
			}()
			select {
			case <-transport.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("contact lookup did not start")
			}
			wantErr := context.DeadlineExceeded
			if !deadline {
				r.stop()
				wantErr = context.Canceled
			}
			select {
			case err := <-transport.cancelled:
				if !errors.Is(err, wantErr) {
					t.Fatalf("lookup context error = %v, want %v", err, wantErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("contact lookup ignored recorder cancellation/deadline")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("write did not return after contact cancellation")
			}
			srv.mu.Lock()
			defer srv.mu.Unlock()
			if len(srv.recordReq) != 0 {
				t.Fatal("record sent after write context ended")
			}
		})
	}
}

func TestResolveUserInfo_CancelledLookupPreservesCachedName(t *testing.T) {
	p, _ := newTurnRecordFixture(t)
	p.userNameCache.Store("ou_user", "Cached name")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	userID, name := p.resolveUserInfo(ctx, "ou_user")
	if userID != "" || name != "Cached name" {
		t.Fatalf("cancelled lookup lost cached fallback: userID=%q name=%q", userID, name)
	}
}

func TestParseTurnRecordOptions_EnrichmentMappings(t *testing.T) {
	cfg, err := parseTurnRecordOptions(map[string]any{"turn_record_fields": map[string]any{
		"requester": " 提问人 ", "trace": "Trace", "duration": "耗时（秒）", "tool_count": "工具次数", "summary": "摘要",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.fields.Requester != "提问人" || cfg.fields.Trace != "Trace" || cfg.fields.Duration != "耗时（秒）" || cfg.fields.ToolCount != "工具次数" || cfg.fields.Summary != "摘要" {
		t.Fatalf("enrichment mappings not applied: %+v", cfg.fields)
	}
	for _, fields := range []map[string]any{
		{"requester": true},
		{"duration": 2},
		{"requester": "昵称"},
		{"trace": "Trace", "summary": "Trace"},
	} {
		if _, err := parseTurnRecordOptions(map[string]any{"turn_record_fields": fields}); err == nil {
			t.Fatalf("invalid mapping accepted: %+v", fields)
		}
	}
	cfg, err = parseTurnRecordOptions(map[string]any{"turn_record_fields": map[string]any{"requester": " ", "trace": ""}})
	if err != nil || cfg.fields.Requester != "" || cfg.fields.Trace != "" {
		t.Fatalf("empty enrichment mappings must stay disabled: cfg=%+v err=%v", cfg, err)
	}
}

func TestTurnRecorderWrite_EnrichmentUsesNativeTypesWithoutContactPermission(t *testing.T) {
	p, srv := newTurnRecordFixture(t, map[string]any{"turn_record_fields": map[string]any{
		"requester": "提问人", "trace": "Trace", "duration": "耗时（秒）", "tool_count": "工具次数", "summary": "摘要",
	}})
	srv.mu.Lock()
	srv.contactCode = 99991672
	srv.mu.Unlock()
	rec := core.TurnRecord{
		At: time.Now(), UserID: "ou_user", UserName: "ou_user", FeishuUserID: "u_legacy",
		Question: "Question", Answer: "Answer", MessageID: "om_enriched",
		TraceURL: "https://trace.example.test/project/demo/traces/trace-123",
		Duration: 1250 * time.Millisecond, ToolCount: 3, Summary: "Verified short summary",
	}
	p.turnRecording.write(p, rec)
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.recordReq) != 1 {
		t.Fatalf("Contact denial must not prevent archival: records=%d", len(srv.recordReq))
	}
	fields := srv.recordReq[0]["fields"].(map[string]any)
	person, ok := fields["提问人"].([]any)
	if !ok || len(person) != 1 {
		t.Fatalf("person field must be an array: %+v", fields["提问人"])
	}
	personValue, ok := person[0].(map[string]any)
	if !ok || len(personValue) != 1 || personValue["id"] != rec.UserID || srv.userIDTypes[0] != "open_id" {
		t.Fatalf("wrong person ID type/value: person=%+v user_id_type=%v", person, srv.userIDTypes)
	}
	if fields["昵称"] != "" || fields["user_id"] != "u_legacy" {
		t.Fatalf("ID leaked into nickname or legacy user_id changed meaning: %+v", fields)
	}
	link, ok := fields["Trace"].(map[string]any)
	if !ok || len(link) != 2 || link["text"] != "Trace" || link["link"] != rec.TraceURL {
		t.Fatalf("invalid URL field structure: %+v", fields["Trace"])
	}
	if fields["耗时（秒）"] != 1.25 || fields["工具次数"] != float64(3) || fields["摘要"] != rec.Summary {
		t.Fatalf("number/text fields have wrong values or types: %+v", fields)
	}
}

func TestTurnRecorderWrite_EnrichmentOmitsMissingDataAndInvalidPersonIDs(t *testing.T) {
	for _, userID := range []string{"u_tenant", "on_union", "oc_chat", "ou_", "ou_bad/id", "ou_user\n"} {
		t.Run(userID, func(t *testing.T) {
			p, srv := newTurnRecordFixture(t, map[string]any{"turn_record_fields": map[string]any{
				"requester": "提问人", "trace": "Trace", "duration": "耗时（秒）", "tool_count": "工具次数", "summary": "摘要",
			}})
			srv.mu.Lock()
			srv.contactCode = 99991672
			srv.mu.Unlock()
			p.turnRecording.write(p, core.TurnRecord{UserID: userID, At: time.Now(), MessageID: "om_missing", Answer: "answer"})
			srv.mu.Lock()
			defer srv.mu.Unlock()
			if len(srv.recordReq) != 1 {
				t.Fatalf("missing metadata should not prevent archival: records=%d", len(srv.recordReq))
			}
			fields := srv.recordReq[0]["fields"].(map[string]any)
			for _, column := range []string{"提问人", "Trace", "耗时（秒）", "摘要"} {
				if _, ok := fields[column]; ok {
					t.Fatalf("missing/invalid field %q must be omitted: %+v", column, fields[column])
				}
			}
			if fields["工具次数"] != float64(0) || fields["user_id"] != "" || fields["昵称"] != "" {
				t.Fatalf("zero tools should be numeric; unresolved identity must stay empty: %+v", fields)
			}
		})
	}
}

func TestTurnRecordDisplayNameRejectsIdentityFallbacks(t *testing.T) {
	for _, test := range []struct{ name, want string }{
		{"ou_user", ""}, {"on_union", ""}, {"u_tenant", ""}, {"  ", ""},
		{"  Alice  ", "Alice"}, {"ou_ team", "ou_ team"},
	} {
		if got := turnRecordDisplayName(test.name, "ou_user", "u_tenant"); got != test.want {
			t.Fatalf("displayName(%q)=%q, want %q", test.name, got, test.want)
		}
	}
}

func TestTurnRecordEnrichmentConfigContract(t *testing.T) {
	for _, option := range feishuConfigOptions("https://open.feishu.cn") {
		if option.Key != "turn_record_fields" {
			continue
		}
		for _, key := range []string{"requester", "trace", "duration", "tool_count", "summary"} {
			if !strings.Contains(option.Description, key) || !strings.Contains(option.DescriptionZH, key) || !strings.Contains(option.Example, key+" = ") {
				t.Fatalf("configuration contract missing %q", key)
			}
		}
		if !strings.Contains(option.Default, "empty") || !strings.Contains(option.Description, "type 11") || !strings.Contains(option.Description, "type 15") || !strings.Contains(option.Description, "seconds") {
			t.Fatalf("contract must describe opt-in defaults and native types: %+v", option)
		}
		return
	}
	t.Fatal("turn_record_fields contract missing")
}
