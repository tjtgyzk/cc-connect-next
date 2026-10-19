package feishu

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	"github.com/timmyagentic/cc-connect-next/core"
)

func questionTestPlatform(t *testing.T) *interactivePlatform {
	t.Helper()
	p, err := New(map[string]any{"app_id": "test-app", "app_secret": "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	return p.(*interactivePlatform)
}

func questionTestEvent() *callback.CardActionTriggerEvent {
	return &callback.CardActionTriggerEvent{Event: &callback.CardActionTriggerRequest{
		Operator: &callback.Operator{OpenID: "ou_requester"},
		Context:  &callback.Context{OpenChatID: "oc_test", OpenMessageID: "om_question"},
		Action: &callback.CallBackAction{Value: map[string]any{
			"action":      "askq:0123456789abcdef01234567:0:2",
			"session_key": "feishu:oc_test:root:om_original",
			"askq_label":  "untrusted-label", "askq_question": "untrusted-question",
		}},
	}}
}

func TestQuestionCard_UpdatesOnlyAfterEngineAcceptance(t *testing.T) {
	p := questionTestPlatform(t)
	message := make(chan *core.Message, 1)
	p.handler = func(_ core.Platform, msg *core.Message) { message <- msg }
	done := make(chan *callback.CardActionTriggerResponse, 1)
	go func() { response, _ := p.onCardAction(questionTestEvent()); done <- response }()
	var msg *core.Message
	select {
	case msg = <-message:
	case <-time.After(time.Second):
		t.Fatal("callback was not dispatched")
	}
	if !msg.IsQuestionResponse || !msg.IsCardAction || msg.QuestionResponse == nil {
		t.Fatalf("missing question acknowledgement contract: %+v", msg)
	}
	if msg.UserID != "ou_requester" || msg.SessionKey != "feishu:oc_test:root:om_original" {
		t.Fatalf("lost callback identity/routing: %+v", msg)
	}
	select {
	case response := <-done:
		t.Fatalf("card reported before acceptance: %+v", response)
	default:
	}
	msg.QuestionResponse <- core.QuestionResponseResult{Accepted: true, Question: "选择环境", Answer: "stg"}
	select {
	case response := <-done:
		if response == nil || response.Card == nil {
			t.Fatalf("missing accepted card: %+v", response)
		}
		body, err := json.Marshal(response.Card.Data)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range []string{"green", "选择环境", "stg"} {
			if !strings.Contains(string(body), value) {
				t.Fatalf("missing %q: %s", value, body)
			}
		}
		if strings.Contains(string(body), "untrusted") || strings.Contains(string(body), "askq:") {
			t.Fatalf("accepted card used callback labels or retained buttons: %s", body)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted card not returned")
	}
}

func TestQuestionCard_RejectedOldClickDoesNotShowSuccess(t *testing.T) {
	p := questionTestPlatform(t)
	p.handler = func(_ core.Platform, msg *core.Message) {
		msg.QuestionResponse <- core.QuestionResponseResult{Message: "问题已失效，请使用最新问题卡。"}
	}
	response, err := p.onCardAction(questionTestEvent())
	if err != nil {
		t.Fatal(err)
	}
	if response == nil || response.Card != nil || response.Toast == nil || response.Toast.Type != "warning" {
		t.Fatalf("rejected click must preserve the card and show warning: %+v", response)
	}
	if response.Toast.Content != "问题已失效，请使用最新问题卡。" {
		t.Fatal(response.Toast.Content)
	}
}

func TestQuestionCard_OperatorBoundary(t *testing.T) {
	for _, kind := range []string{"allow_from", "direct_recipient", "missing_operator"} {
		t.Run(kind, func(t *testing.T) {
			p := questionTestPlatform(t)
			called := make(chan struct{}, 1)
			p.handler = func(_ core.Platform, _ *core.Message) { called <- struct{}{} }
			event := questionTestEvent()
			switch kind {
			case "allow_from":
				p.allowFrom = "ou_other"
			case "direct_recipient":
				event.Event.Action.Value[cardActionDirectUserKey] = "ou_other"
			case "missing_operator":
				event.Event.Operator = nil
			}
			response, err := p.onCardAction(event)
			if err != nil {
				t.Fatal(err)
			}
			if response != nil && response.Card != nil {
				t.Fatal("unauthorized operator got an accepted card")
			}
			select {
			case <-called:
				t.Fatal("unauthorized operator reached engine")
			default:
			}
		})
	}
}

type questionUpdateCapture struct {
	*interactivePlatform
	updates chan questionUpdate
}
type questionUpdate struct {
	rc   replyContext
	card *core.Card
}

func (p *questionUpdateCapture) UpdateCard(_ context.Context, rc any, card *core.Card) error {
	p.updates <- questionUpdate{rc: rc.(replyContext), card: card}
	return nil
}

func TestQuestionCard_LateAcceptanceUpdatesOnlyOriginalMessage(t *testing.T) {
	p := questionTestPlatform(t)
	capture := &questionUpdateCapture{interactivePlatform: p, updates: make(chan questionUpdate, 1)}
	p.self = capture
	ack := make(chan core.QuestionResponseResult, 1)
	rc := replyContext{messageID: "om_original", sessionKey: "feishu:oc_test:root:om_original", chatID: "oc_test"}
	response := p.awaitQuestionCardAck(ack, rc, map[string]any{"question_processing": "正在确认"}, time.Millisecond)
	if response.Card != nil || response.Toast == nil || response.Toast.Content != "正在确认" {
		t.Fatalf("pending acknowledgement shown as success: %+v", response)
	}
	// A newer card being displayed must not redirect this delayed update.
	p.cardActionMsgMu.Lock()
	p.cardActionTargets = map[string]cardActionTargetSnapshot{rc.sessionKey: {messageID: "om_newer"}}
	p.cardActionMsgMu.Unlock()
	ack <- core.QuestionResponseResult{Accepted: true, Question: "选择环境", Answer: "prod"}
	select {
	case update := <-capture.updates:
		if update.rc.messageID != "om_original" || !strings.Contains(update.card.RenderText(), "prod") {
			t.Fatalf("wrong late update: %+v", update)
		}
	case <-time.After(time.Second):
		t.Fatal("late acceptance did not update original message")
	}
}

func TestQuestionCard_LateRejectionNeverUpdatesCard(t *testing.T) {
	p := questionTestPlatform(t)
	capture := &questionUpdateCapture{interactivePlatform: p, updates: make(chan questionUpdate, 1)}
	p.self = capture
	ack := make(chan core.QuestionResponseResult, 1)
	ack <- core.QuestionResponseResult{Message: "Expired"}
	p.updateQuestionCardAfterAck(ack, replyContext{messageID: "om_old"})
	select {
	case <-capture.updates:
		t.Fatal("rejected click updated card")
	default:
	}
}
