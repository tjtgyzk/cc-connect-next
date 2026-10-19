package feishu

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	"github.com/timmyagentic/cc-connect-next/core"
)

func questionMentionTestCard() *core.Card {
	card := core.NewCard().Title("Choose environment", "blue").
		Markdown("**Which environment?**").
		ListItemBtnExtra("Production", "prod", "primary", "askq:test:0:1", map[string]string{"askq_label": "prod"}).Build()
	card.NotifyReplySender = true
	return card
}

func TestQuestionCardMentionRequiresVerifiedSenderAndFirstQuestion(t *testing.T) {
	p := &Platform{}
	trusted := replyContext{messageID: "om_trigger", chatID: "oc_group", sessionKey: "feishu:oc_group:root:om_trigger", replyMentionID: "ou_requester"}
	for _, tc := range []struct {
		name   string
		rc     replyContext
		notify bool
		want   bool
	}{
		{"first question", trusted, true, true},
		{"later question", trusted, false, false},
		{"missing verified user", replyContext{messageID: "om_trigger", chatID: "oc_group"}, true, false},
		{"proactive context", replyContext{chatID: "oc_group", replyMentionID: "ou_requester"}, true, false},
		{"missing chat", replyContext{messageID: "om_trigger", replyMentionID: "ou_requester"}, true, false},
		{"all is invalid", replyContext{messageID: "om_trigger", chatID: "oc_group", replyMentionID: "all"}, true, false},
		{"invalid ID", replyContext{messageID: "om_trigger", chatID: "oc_group", replyMentionID: "ou_sender></at><at id=all"}, true, false},
		{"direct recipient is not sender", replyContext{messageID: "om_trigger", chatID: "oc_direct", directUserID: "ou_recipient"}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			card := questionMentionTestCard()
			card.NotifyReplySender = tc.notify
			got := p.renderNewReplyCard(card, tc.rc)
			var rendered struct {
				Config struct {
					Summary struct {
						Content string `json:"content"`
					} `json:"summary"`
				} `json:"config"`
				Elements []struct {
					Content string `json:"content"`
				} `json:"elements"`
			}
			if err := json.Unmarshal([]byte(got), &rendered); err != nil {
				t.Fatal(err)
			}
			notified := len(rendered.Elements) > 0 && rendered.Elements[0].Content == "<at id=ou_requester></at>"
			if notified != tc.want {
				t.Fatalf("notified=%v want=%v: %s", notified, tc.want, got)
			}
			if tc.want && rendered.Config.Summary.Content != "Which environment?" {
				t.Fatalf("summary=%q", rendered.Config.Summary.Content)
			}
			if strings.Contains(got, "id=all") {
				t.Fatal("must never mention all")
			}
			if len(card.Elements) != 2 || card.Elements[0].(core.CardMarkdown).Content != "**Which environment?**" {
				t.Fatal("render mutated shared card")
			}
			if !strings.Contains(got, "askq:test:0:1") {
				t.Fatal("question action lost")
			}
		})
	}
}

func TestQuestionCardNotificationOnlyOnNewMessage(t *testing.T) {
	for _, tc := range []struct {
		name     string
		send     bool
		topic    bool
		noReply  bool
		wantPath string
	}{
		{"reply", false, false, false, "/open-apis/im/v1/messages/om_trigger/reply"},
		{"send", true, false, false, "/open-apis/im/v1/messages"},
		{"reply without quote", false, false, true, "/open-apis/im/v1/messages"},
		{"topic send", true, true, false, "/open-apis/im/v1/messages/om_trigger/reply"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var contents []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
					writeJSON(t, w, map[string]any{"code": 0, "expire": 7200, "tenant_access_token": "test-token"})
					return
				}
				if len(contents) == 0 && (r.Method != http.MethodPost || r.URL.Path != tc.wantPath) {
					t.Errorf("create=%s %s", r.Method, r.URL.Path)
				}
				if len(contents) == 1 && (r.Method != http.MethodPatch || r.URL.Path != "/open-apis/im/v1/messages/om_trigger") {
					t.Errorf("patch=%s %s", r.Method, r.URL.Path)
				}
				var body struct {
					Content       string `json:"content"`
					ReplyInThread bool   `json:"reply_in_thread"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if len(contents) == 0 && tc.topic && !body.ReplyInThread {
					t.Error("topic routing lost")
				}
				contents = append(contents, body.Content)
				writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"message_id": "om_created"}})
			}))
			defer srv.Close()
			p := &interactivePlatform{Platform: &Platform{
				platformName: "feishu", domain: srv.URL, appID: "test-app", appSecret: "test-secret", noReplyToTrigger: tc.noReply,
				client: lark.NewClient("test-app", "test-secret", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client())),
			}}
			rc := replyContext{messageID: "om_trigger", chatID: "oc_group", replyMentionID: "ou_requester", sessionKey: "feishu:oc_group:ou_requester"}
			if tc.topic {
				rc.sessionKey = "feishu:oc_group:root:om_trigger"
			}
			card := questionMentionTestCard()
			var err error
			if tc.send {
				err = p.SendCard(context.Background(), rc, card)
			} else {
				err = p.ReplyCard(context.Background(), rc, card)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = p.UpdateCard(context.Background(), rc, card); err != nil {
				t.Fatal(err)
			}
			if len(contents) != 2 {
				t.Fatalf("message operations=%d want=2", len(contents))
			}
			// Decode JSON escapes before checking native at markup.
			for i, content := range contents {
				var decoded any
				if err := json.Unmarshal([]byte(content), &decoded); err != nil {
					t.Fatal(err)
				}
				b, _ := json.Marshal(decoded)
				hasMention := strings.Contains(string(b), "id=ou_requester")
				if hasMention != (i == 0) {
					t.Fatalf("operation %d has mention=%v", i, hasMention)
				}
			}
		})
	}
}
