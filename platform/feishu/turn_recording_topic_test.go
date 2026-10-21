package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	"github.com/timmyagentic/cc-connect-next/core"
)

func topicArchiveOptions() map[string]any {
	return map[string]any{
		"turn_record_mode": "topic", "turn_record_app_token": "base_test", "turn_record_table_id": "table_test",
		"turn_record_fields": map[string]any{
			"task_id": "任务ID", "turn_count": "交互轮数", "conversation": "完整对话", "trace_history": "历史Trace",
			"archive_state": "归档数据", "record_kind": "归档类型", "duration": "耗时", "tool_count": "工具次数",
			"trace": "Trace", "requester": "提问人", "summary": "任务",
		},
	}
}

type topicArchiveServer struct {
	mu                         sync.Mutex
	rows                       map[string]map[string]any
	types                      map[string]int
	creates, updates, searches int
	clientTokens               []string
	unknownWrite, dropWrite    int
	searchRichText             bool
}

func (s *topicArchiveServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	respond := func(data any) { _ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "msg": "ok", "data": data}) }
	switch {
	case strings.Contains(r.URL.Path, "/auth/v3/tenant_access_token/internal"):
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "synthetic", "expire": 7200})
	case strings.HasSuffix(r.URL.Path, "/fields"):
		fields := []map[string]any{}
		for name, kind := range s.types {
			fields = append(fields, map[string]any{"field_name": name, "type": kind})
		}
		respond(map[string]any{"items": fields, "has_more": false})
	case strings.HasSuffix(r.URL.Path, "/records/search"):
		s.searches++
		var body struct {
			Filter struct {
				Conditions []struct {
					FieldName string   `json:"field_name"`
					Operator  string   `json:"operator"`
					Value     []string `json:"value"`
				} `json:"conditions"`
			} `json:"filter"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Filter.Conditions) != 1 || body.Filter.Conditions[0].FieldName != "任务ID" || body.Filter.Conditions[0].Operator != "is" || len(body.Filter.Conditions[0].Value) != 1 {
			http.Error(w, "invalid exact task search", 400)
			return
		}
		rows := []map[string]any{}
		for id, fields := range s.rows {
			if fields["任务ID"] != body.Filter.Conditions[0].Value[0] {
				continue
			}
			result := map[string]any{"任务ID": fields["任务ID"], "归档数据": fields["归档数据"]}
			if s.searchRichText {
				for key, value := range result {
					result[key] = []map[string]any{{"type": "text", "text": value}}
				}
			}
			rows = append(rows, map[string]any{"record_id": id, "fields": result})
		}
		respond(map[string]any{"items": rows, "has_more": false, "total": len(rows)})
	case strings.Contains(r.URL.Path, "/records") && (r.Method == "POST" || r.Method == "PUT"):
		var body struct {
			Fields map[string]any `json:"fields"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad fields", 400)
			return
		}
		id := ""
		if r.Method == "POST" {
			s.creates++
			id = fmt.Sprintf("rec_%d", s.creates)
			s.clientTokens = append(s.clientTokens, r.URL.Query().Get("client_token"))
		} else {
			s.updates++
			parts := strings.Split(r.URL.Path, "/")
			id = parts[len(parts)-1]
		}
		if s.dropWrite > 0 {
			s.dropWrite--
			_, _ = w.Write([]byte("{"))
			return
		}
		s.rows[id] = body.Fields
		if s.unknownWrite > 0 {
			s.unknownWrite--
			_, _ = w.Write([]byte("{"))
			return
		}
		respond(map[string]any{"record": map[string]any{"record_id": id, "fields": body.Fields}})
	default:
		http.Error(w, "unexpected endpoint", 404)
	}
}

func newTopicArchiveFixture(t *testing.T) (*Platform, *turnRecorder, *topicArchiveServer) {
	t.Helper()
	cfg, err := parseTurnRecordOptions(topicArchiveOptions())
	if err != nil {
		t.Fatal(err)
	}
	recorder := newTurnRecorder(cfg)
	t.Cleanup(recorder.stop)
	fixture := &topicArchiveServer{rows: make(map[string]map[string]any), types: recorder.topicArchiveFieldTypes()}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	t.Cleanup(server.Close)
	p := &Platform{platformName: "feishu", client: lark.NewClient("cli_topic_"+t.Name(), "synthetic", lark.WithOpenBaseUrl(server.URL), lark.WithHttpClient(server.Client()))}
	return p, recorder, fixture
}

func topicArchiveExample(index int) topicArchiveTurn {
	return topicArchiveTurn{Record: core.TurnRecord{
		At:     time.Date(2026, 10, 21, 9, index, 0, 0, time.FixedZone("CST", 8*60*60)),
		UserID: "ou_test", UserName: "Alice", ChatName: "Test group", Model: "test-model",
		SessionKey: "feishu:oc_group:root:om_root", ConversationID: "conversation_1",
		Question: fmt.Sprintf("question %d", index), Answer: fmt.Sprintf("answer %d", index), Summary: fmt.Sprintf("task %d", index),
		MessageID: fmt.Sprintf("om_%d", index), Duration: time.Duration(index) * time.Second, ToolCount: index,
		AgentSessionID: "native_session", AgentTurnID: fmt.Sprintf("turn_%d", index), TraceURL: fmt.Sprintf("https://trace.example.test/%d", index),
	}, DisplayName: "Alice", TenantUserID: "user_test"}
}

func TestTopicArchive_ThreeFollowupsOneMainRecordAndRestartDedup(t *testing.T) {
	p, recorder, fixture := newTopicArchiveFixture(t)
	for i := 1; i <= 3; i++ {
		turn := topicArchiveExample(i)
		if i == 2 {
			turn.DisplayName = "Bob"
			turn.Record.UserID = "ou_other"
			turn.Record.UserName = "Bob"
		}
		if err := recorder.writeTopic(context.Background(), p, "base_test", turn); err != nil {
			t.Fatal(err)
		}
	}
	if len(fixture.rows) != 1 || fixture.creates != 1 || fixture.updates != 2 {
		t.Fatalf("rows/writes = %d/%d/%d", len(fixture.rows), fixture.creates, fixture.updates)
	}
	row := fixture.rows["rec_1"]
	for key, want := range map[string]any{"问题": "question 1", "任务": "task 1", "答案": "answer 3", "交互轮数": float64(3), "耗时": float64(6), "工具次数": float64(6), "归档类型": "主记录", "昵称": "Alice", "时间": "2026-10-21 09:03:00"} {
		if row[key] != want {
			t.Fatalf("%s = %v, want %v", key, row[key], want)
		}
	}
	conversation := row["完整对话"].(string)
	for i := 1; i <= 3; i++ {
		if !strings.Contains(conversation, fmt.Sprintf("question %d", i)) || !strings.Contains(conversation, fmt.Sprintf("answer %d", i)) || !strings.Contains(row["历史Trace"].(string), fmt.Sprintf("https://trace.example.test/%d", i)) {
			t.Fatal("lost a question, answer or trace")
		}
	}
	if !strings.Contains(conversation, "Bob") {
		t.Fatal("lost followup requester")
	}
	if row["Trace"].(map[string]any)["link"] != "https://trace.example.test/3" {
		t.Fatal("latest trace missing")
	}
	state, err := decodeTopicArchive(row["归档数据"].(string), "conversation_1")
	if err != nil || len(state.Turns) != 3 {
		t.Fatalf("state = %+v, %v", state, err)
	}
	// No in-memory task map: a new recorder recovers exact state from the row.
	restarted := newTurnRecorder(recorder.cfg)
	defer restarted.stop()
	fixture.searchRichText = true
	if err := restarted.writeTopic(context.Background(), p, "base_test", topicArchiveExample(3)); err != nil {
		t.Fatal(err)
	}
	if fixture.creates != 1 || fixture.updates != 2 {
		t.Fatal("restart retry duplicated a delivered turn")
	}
}

func TestTopicArchive_SeparateTopicsNewSessionsAndNonTopicMessages(t *testing.T) {
	p, recorder, fixture := newTopicArchiveFixture(t)
	turns := []topicArchiveTurn{topicArchiveExample(1), topicArchiveExample(2), topicArchiveExample(3), topicArchiveExample(4)}
	turns[1].Record.ConversationID = "conversation_after_new"
	for i := 2; i < 4; i++ {
		turns[i].Record.SessionKey = "feishu:oc_group:ou_test"
	}
	for _, turn := range turns {
		if err := recorder.writeTopic(context.Background(), p, "base_test", turn); err != nil {
			t.Fatal(err)
		}
	}
	if len(fixture.rows) != 4 {
		t.Fatalf("unrelated tasks merged: %d rows", len(fixture.rows))
	}
}

func TestTopicArchive_UnknownWriteOutcomeRereadsBeforeRetry(t *testing.T) {
	for _, operation := range []string{"create", "update", "dropped update"} {
		t.Run(operation, func(t *testing.T) {
			p, recorder, fixture := newTopicArchiveFixture(t)
			turn := topicArchiveExample(1)
			if operation != "create" {
				if err := recorder.writeTopic(context.Background(), p, "base_test", turn); err != nil {
					t.Fatal(err)
				}
				turn = topicArchiveExample(2)
			}
			if operation == "dropped update" {
				fixture.dropWrite = 1
			} else {
				fixture.unknownWrite = 1
			}
			if err := recorder.writeTopic(context.Background(), p, "base_test", turn); err != nil {
				t.Fatal(err)
			}
			if len(fixture.rows) != 1 || fixture.creates != 1 {
				t.Fatal("ambiguous result created duplicate record")
			}
			wantTurns := float64(1)
			if operation != "create" {
				wantTurns = 2
			}
			if fixture.rows["rec_1"]["交互轮数"] != wantTurns {
				t.Fatal("ambiguous result double-counted turn")
			}
			wantUpdates := 0
			if operation == "update" {
				wantUpdates = 1
			}
			if operation == "dropped update" {
				wantUpdates = 2
			}
			if fixture.updates != wantUpdates {
				t.Fatalf("updates = %d, want %d", fixture.updates, wantUpdates)
			}
		})
	}
}

func TestTopicArchive_RefusesConflictsBadStateDuplicateTasksAndWrongTypes(t *testing.T) {
	for _, scenario := range []string{"conflicting turn", "duplicate task", "malformed state", "state version", "state identity", "duplicate turn", "wrong column type", "missing column", "oversized conversation", "missing conversation ID"} {
		t.Run(scenario, func(t *testing.T) {
			p, recorder, fixture := newTopicArchiveFixture(t)
			if err := recorder.writeTopic(context.Background(), p, "base_test", topicArchiveExample(1)); err != nil {
				t.Fatal(err)
			}
			turn := topicArchiveExample(2)
			row := fixture.rows["rec_1"]
			switch scenario {
			case "conflicting turn":
				turn = topicArchiveExample(1)
				turn.Record.Answer = "different content"
			case "duplicate task":
				fixture.rows["rec_duplicate"] = row
			case "malformed state":
				row["归档数据"] = "{broken"
			case "state version":
				row["归档数据"] = strings.Replace(row["归档数据"].(string), `"version":1`, `"version":2`, 1)
			case "state identity":
				row["归档数据"] = strings.Replace(row["归档数据"].(string), `"task_id":"conversation_1"`, `"task_id":"other"`, 1)
			case "duplicate turn":
				state, _ := decodeTopicArchive(row["归档数据"].(string), "conversation_1")
				state.Turns = append(state.Turns, state.Turns[0])
				raw, _ := json.Marshal(state)
				row["归档数据"] = string(raw)
			case "wrong column type":
				fixture.types["归档数据"] = 2
			case "missing column":
				delete(fixture.types, "归档类型")
			case "oversized conversation":
				turn.Record.Answer = strings.Repeat("x", topicArchiveTextLimit)
			case "missing conversation ID":
				turn.Record.ConversationID = ""
			}
			before, _ := json.Marshal(fixture.rows)
			if err := recorder.writeTopic(context.Background(), p, "base_test", turn); err == nil {
				t.Fatal("unsafe archive update accepted")
			}
			after, _ := json.Marshal(fixture.rows)
			if !reflect.DeepEqual(before, after) || fixture.updates != 0 || fixture.creates != 1 {
				t.Fatal("bad state was overwritten")
			}
		})
	}
}

func TestTopicArchive_TurnIdentityPrefersNativeAndFallsBackToMessage(t *testing.T) {
	first := topicArchiveExample(1).Record
	second := first
	second.AgentTurnID = "another_turn"
	a, _ := topicArchiveTurnKey(first)
	b, _ := topicArchiveTurnKey(second)
	if a == b {
		t.Fatal("distinct native turns sharing a trigger were swallowed")
	}
	first.AgentTurnID = ""
	second.AgentTurnID = ""
	second.AgentSessionID = "other_session"
	a, _ = topicArchiveTurnKey(first)
	b, _ = topicArchiveTurnKey(second)
	if a != b || !strings.HasPrefix(a, "message:") {
		t.Fatal("message fallback unstable")
	}
	first.MessageID = ""
	if _, err := topicArchiveTurnKey(first); err == nil {
		t.Fatal("missing identity accepted")
	}
}

func TestParseTurnRecordTopicOptionsAndContract(t *testing.T) {
	cfg, err := parseTurnRecordOptions(map[string]any{})
	if err != nil || cfg.mode != "turn" {
		t.Fatal("legacy omitted mode changed")
	}
	for _, value := range []any{true, "", "conversation"} {
		if _, err := parseTurnRecordOptions(map[string]any{"turn_record_mode": value}); err == nil {
			t.Fatalf("accepted invalid mode %v", value)
		}
	}
	for _, key := range []string{"task_id", "turn_count", "conversation", "trace_history", "archive_state", "record_kind"} {
		opts := topicArchiveOptions()
		delete(opts["turn_record_fields"].(map[string]any), key)
		if _, err := parseTurnRecordOptions(opts); err == nil {
			t.Fatalf("accepted missing topic %s", key)
		}
		opts = topicArchiveOptions()
		opts["turn_record_fields"].(map[string]any)[key] = 1
		if _, err := parseTurnRecordOptions(opts); err == nil {
			t.Fatalf("accepted wrong topic %s type", key)
		}
	}
	found := false
	for _, option := range feishuConfigOptions("https://open.feishu.cn") {
		if option.Key == "turn_record_mode" {
			found = true
			if option.Default != "turn" || !reflect.DeepEqual(option.Values, []string{"turn", "topic"}) || !strings.Contains(option.DescriptionZH, "追加") && !strings.Contains(strings.Join(option.Keywords, " "), "追加问题") {
				t.Fatalf("mode contract invalid: %+v", option)
			}
		}
	}
	if !found {
		t.Fatal("mode missing from compiled contract")
	}
}

func TestTopicArchive_StaticValidatorRejectsIncompleteOrAliasedMappings(t *testing.T) {
	for _, invalid := range []string{"missing", "alias", "type", "base alias"} {
		opts := topicArchiveOptions()
		opts["app_id"], opts["app_secret"] = "synthetic", "synthetic"
		fields := opts["turn_record_fields"].(map[string]any)
		switch invalid {
		case "missing":
			delete(fields, "task_id")
		case "alias":
			fields["archive_state"] = "问题"
		case "type":
			fields["question"] = 42
		case "base alias":
			fields["question"] = "答案"
		}
		if err := validatePlatformOptions("feishu")(opts); err == nil {
			t.Fatalf("validator accepted %s", invalid)
		}
	}
}

func TestTopicArchive_TranscriptLabelsSupportAllLanguages(t *testing.T) {
	_, recorder, _ := newTopicArchiveFixture(t)
	for _, language := range []core.Language{core.LangEnglish, core.LangChinese, core.LangTraditionalChinese, core.LangJapanese, core.LangSpanish} {
		turn := topicArchiveExample(1)
		turn.Record.Language = language
		turn.Key, _ = topicArchiveTurnKey(turn.Record)
		fields, err := recorder.topicArchiveFields(topicArchiveState{Version: 1, TaskID: "conversation_1", Turns: []topicArchiveTurn{turn}})
		if err != nil {
			t.Fatal(err)
		}
		i18n := core.NewI18n(language)
		for _, label := range []string{i18n.Tf(core.MsgTopicArchiveTurn, 1), i18n.T(core.MsgTopicArchiveQuestion), i18n.T(core.MsgTopicArchiveAnswer)} {
			if !strings.Contains(fields["完整对话"].(string), label) {
				t.Fatalf("language %q missing %q", language, label)
			}
		}
		if fields["归档类型"] != topicArchiveMainKind {
			t.Fatal("storage enum was translated")
		}
	}
}

func TestTopicArchive_CreateRetryUsesStableTaskToken(t *testing.T) {
	p, recorder, fixture := newTopicArchiveFixture(t)
	fixture.dropWrite = 1
	if err := recorder.writeTopic(context.Background(), p, "base_test", topicArchiveExample(1)); err != nil {
		t.Fatal(err)
	}
	if len(fixture.rows) != 1 || len(fixture.clientTokens) != 2 || fixture.clientTokens[0] != fixture.clientTokens[1] {
		t.Fatalf("create retry token not stable: %+v", fixture.clientTokens)
	}
}
