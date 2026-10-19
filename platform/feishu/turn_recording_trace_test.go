package feishu

import (
	"strings"
	"testing"
	"time"

	"github.com/timmyagentic/cc-connect-next/core"
)

func TestTurnRecordTraceOptionsValidateTemplateAndPrefix(t *testing.T) {
	for _, test := range []struct {
		name string
		opts map[string]any
	}{
		{"template type", map[string]any{"turn_record_trace_seed_template": true}},
		{"prefix type", map[string]any{"turn_record_trace_url_prefix": 12}},
		{"session missing", map[string]any{"turn_record_trace_seed_template": "{turn_id}:trace:1"}},
		{"turn missing", map[string]any{"turn_record_trace_seed_template": "{session_id}:trace:1"}},
		{"unknown placeholder", map[string]any{"turn_record_trace_seed_template": "{session_id}:{turn_id}:{timestamp}"}},
		{"malformed placeholder", map[string]any{"turn_record_trace_seed_template": "{session_id}:{turn_id}:trace{"}},
		{"relative prefix", map[string]any{"turn_record_trace_url_prefix": "/project/demo/traces"}},
		{"wrong scheme", map[string]any{"turn_record_trace_url_prefix": "file:///trace"}},
		{"missing host", map[string]any{"turn_record_trace_url_prefix": "https:///trace"}},
		{"credentials", map[string]any{"turn_record_trace_url_prefix": "https://user:synthetic-password@example.test/traces"}},
		{"query", map[string]any{"turn_record_trace_url_prefix": "https://example.test/traces?token=synthetic-password"}},
		{"empty query", map[string]any{"turn_record_trace_url_prefix": "https://example.test/traces?"}},
		{"fragment", map[string]any{"turn_record_trace_url_prefix": "https://example.test/traces#fragment"}},
		{"empty fragment", map[string]any{"turn_record_trace_url_prefix": "https://example.test/traces#"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseTurnRecordOptions(test.opts); err == nil {
				t.Fatal("invalid trace options accepted")
			} else if strings.Contains(err.Error(), "synthetic-password") {
				t.Fatal("configuration error leaked URL credentials/query")
			}
		})
	}
}

func TestTurnRecordTraceURLMatchesExporterSeedExactly(t *testing.T) {
	for _, prefix := range []string{"https://trace.example.test/project/demo/traces", "https://trace.example.test/project/demo/traces/"} {
		cfg, err := parseTurnRecordOptions(map[string]any{
			"turn_record_trace_seed_template": "{session_id}:{turn_id}:trace:1",
			"turn_record_trace_url_prefix":    prefix,
		})
		if err != nil {
			t.Fatal(err)
		}
		// Fixed fixtures independently calculated with Python hashlib.sha256.
		want := "https://trace.example.test/project/demo/traces/4837b063428981a77df092ba702696dc"
		if got := cfg.traceURL("session-fixture", "turn-fixture"); got != want {
			t.Fatalf("trace URL = %q, want %q", got, want)
		}
		other := "https://trace.example.test/project/demo/traces/8cae9a588e48194fa24886c10730a923"
		if got := cfg.traceURL("session-fixture", "other-turn"); got != other {
			t.Fatalf("different native turn did not select its exact trace: %q", got)
		}
	}
	cfg, err := parseTurnRecordOptions(map[string]any{
		"turn_record_trace_seed_template": " {session_id}:{turn_id}:trace:1 ",
		"turn_record_trace_url_prefix":    "http://trace.example.test/traces/",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.traceURL("session-fixture", "turn-fixture"); got != "http://trace.example.test/traces/17b6615c65b9877e1ab10c305df420d6" {
		t.Fatalf("literal seed whitespace was changed before hashing: %q", got)
	}
}

func TestTurnRecordTraceURLRequiresBothNativeIDsAndOptions(t *testing.T) {
	for _, opts := range []map[string]any{
		{},
		{"turn_record_trace_seed_template": "", "turn_record_trace_url_prefix": ""},
		{"turn_record_trace_seed_template": "{session_id}:{turn_id}:trace:1"},
		{"turn_record_trace_url_prefix": "https://trace.example.test/traces/"},
	} {
		cfg, err := parseTurnRecordOptions(opts)
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.traceURL("session-fixture", "turn-fixture"); got != "" {
			t.Fatalf("missing config must not generate a trace: %q", got)
		}
	}
	cfg, err := parseTurnRecordOptions(map[string]any{
		"turn_record_trace_seed_template": "{session_id}:{turn_id}:trace:1",
		"turn_record_trace_url_prefix":    "https://trace.example.test/traces/",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][2]string{{"", "turn-fixture"}, {"session-fixture", ""}, {" ", "turn-fixture"}, {"session-fixture", "\t"}} {
		if got := cfg.traceURL(ids[0], ids[1]); got != "" {
			t.Fatalf("missing native ID must not generate a trace: %q", got)
		}
	}
}

func TestTurnRecorderWriteTraceConfigUsesNativeTurnAndExplicitURLPrecedence(t *testing.T) {
	for _, test := range []struct {
		name, explicitURL, sessionID, turnID, want string
	}{
		{"native pair", "", "session-fixture", "turn-fixture", "https://trace.example.test/traces/4837b063428981a77df092ba702696dc"},
		{"explicit URL", "https://trace.example.test/verified/receipt", "session-fixture", "turn-fixture", "https://trace.example.test/verified/receipt"},
		{"no native session", "", "", "turn-fixture", ""},
		{"no native turn", "", "session-fixture", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, srv := newTurnRecordFixture(t, map[string]any{
				"turn_record_trace_seed_template": "{session_id}:{turn_id}:trace:1",
				"turn_record_trace_url_prefix":    "https://trace.example.test/traces/",
				"turn_record_fields":              map[string]any{"trace": "Trace", "duration": "Seconds", "tool_count": "Tools"},
			})
			p.turnRecording.write(p, core.TurnRecord{
				At: time.Now(), UserID: "ou_user", Question: "Question", Answer: "Answer", MessageID: "om_trace",
				SessionKey: "a-routing-key-is-not-a-native-id", AgentSessionID: test.sessionID, AgentTurnID: test.turnID,
				TraceURL: test.explicitURL, Duration: -time.Second,
			})
			srv.mu.Lock()
			defer srv.mu.Unlock()
			if len(srv.recordReq) != 1 {
				t.Fatalf("archival request count = %d", len(srv.recordReq))
			}
			fields := srv.recordReq[0]["fields"].(map[string]any)
			if test.want == "" {
				if _, exists := fields["Trace"]; exists {
					t.Fatalf("generated a trace without both native IDs: %+v", fields["Trace"])
				}
			} else if link, ok := fields["Trace"].(map[string]any); !ok || link["link"] != test.want || link["text"] != "Trace" {
				t.Fatalf("trace link = %+v, want %q", fields["Trace"], test.want)
			}
			if _, exists := fields["Seconds"]; exists {
				t.Fatal("non-positive duration must not be recorded as a measured zero")
			}
			if fields["Tools"] != float64(0) {
				t.Fatalf("zero observed tools must remain numeric zero: %+v", fields["Tools"])
			}
		})
	}
}

func TestTurnRecordTraceOptionsAreDeclaredInConfigContract(t *testing.T) {
	found := map[string]bool{}
	for _, option := range feishuConfigOptions("https://open.feishu.cn") {
		if option.Key != "turn_record_trace_seed_template" && option.Key != "turn_record_trace_url_prefix" {
			continue
		}
		found[option.Key] = true
		if option.Type != "string" || option.Default != "empty" || option.DescriptionZH == "" || len(option.Requires) == 0 || !strings.Contains(option.Example, option.Key+" = ") {
			t.Fatalf("trace configuration contract incomplete: %+v", option)
		}
	}
	if len(found) != 2 {
		t.Fatalf("missing trace configuration option: %+v", found)
	}
}
