package feishu

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/timmyagentic/cc-connect-next/core"
)

func TestFinalCardMentionRequiresVerifiedTriggerSender(t *testing.T) {
	p := &interactivePlatform{Platform: &Platform{}}
	trusted := replyContext{messageID: "om_question", chatID: "oc_group", replyMentionID: "ou_sender"}
	for _, tc := range []struct {
		name string
		ctx  any
		body string
		want bool
	}{
		{"sender table answer", trusted, "<at id=ou_sender></at>\n\n|字段|值|\n|---|---|\n|测试|通过|", true},
		{"quoted sender", trusted, `<at id="ou_sender"></at> 完成`, true},
		{"single quoted sender", trusted, "<at id='ou_sender'></at> 完成", true},
		{"leading newlines", trusted, "\n <at id=ou_sender></at> 完成", true},
		{"ordinary answer", trusted, "完成", false},
		{"empty answer", trusted, "", false},
		{"only newlines", trusted, "\r\n\n", false},
		{"another user", trusted, "<at id=ou_other></at> 完成", false},
		{"all members", trusted, "<at id=all></at> 完成", false},
		{"additional tag", trusted, "<at id=ou_sender></at> <at id=ou_other></at>", false},
		{"text tag", trusted, `<at user_id="ou_sender">测试</at> 完成`, false},
		{"code example", trusted, "`<at id=ou_sender></at>`", false},
		{"fenced code", trusted, "```\n<at id=ou_sender></at>\n```", false},
		{"indented code", trusted, "    <at id=ou_sender></at>", false},
		{"escaped tag", trusted, `\<at id=ou_sender></at>`, false},
		{"no verified user", replyContext{messageID: "om_question", chatID: "oc_group"}, "<at id=ou_sender></at> 完成", false},
		{"proactive reply", replyContext{chatID: "oc_group", replyMentionID: "ou_sender"}, "<at id=ou_sender></at> 完成", false},
		{"invalid context", "ctx", "<at id=ou_sender></at> 完成", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.RequiresNewFinalCard(context.Background(), tc.ctx, tc.body); got != tc.want {
				t.Fatalf("RequiresNewFinalCard = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCompletedMentionCardKeepsBodyAndHasPlainSummary(t *testing.T) {
	markdown := "<at id=ou_sender></at>\n\n已完成查询。\n\n|字段|值|\n|---|---|\n|测试|通过|"
	var card struct {
		Config struct {
			Summary struct {
				Content string `json:"content"`
			} `json:"summary"`
		} `json:"config"`
		Body struct {
			Elements []struct {
				Content string `json:"content"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(buildRichCard(core.CardStatusDone, "done", nil, markdown, false, "")), &card); err != nil {
		t.Fatal(err)
	}
	if card.Config.Summary.Content != "已完成查询。" {
		t.Fatalf("summary=%q", card.Config.Summary.Content)
	}
	if len(card.Body.Elements) == 0 || !strings.Contains(card.Body.Elements[0].Content, "<at id=ou_sender></at>") || !strings.Contains(card.Body.Elements[0].Content, "|字段|值|") {
		t.Fatalf("card body must retain mention and table")
	}
	if got := richCardSummary("<at id=ou_sender></at>", "已完成"); got != "已完成" {
		t.Fatalf("empty summary fallback=%q", got)
	}
	if got := richCardSummary(strings.Repeat("测", 130), "已完成"); len([]rune(got)) != 120 {
		t.Fatalf("summary length=%d", len([]rune(got)))
	}
}
