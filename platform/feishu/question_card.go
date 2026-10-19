package feishu

import (
	"context"
	"log/slog"
	"time"

	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	"github.com/timmyagentic/cc-connect-next/core"
)

const questionCardAckTimeout = time.Second

// A callback may arrive more than once, after another question was displayed,
// or after the native request has ended. Only the engine owns that decision;
// labels supplied by the card are never used as evidence of acceptance.
func (p *Platform) handleQuestionCardAction(action, userID string, rc replyContext, values map[string]any) *callback.CardActionTriggerResponse {
	unavailable := questionCardCopy(values, "question_unavailable", core.MsgQuestionExpired)
	if userID == "" || !core.AllowList(p.allowFrom, userID) ||
		(rc.directUserID != "" && rc.directUserID != userID) || p.getHandler() == nil {
		return questionCardToast("warning", unavailable)
	}
	ack := make(chan core.QuestionResponseResult, 1)
	go p.dispatchCoreMessage(&core.Message{
		SessionKey:         rc.sessionKey,
		Platform:           p.platformName,
		UserID:             userID,
		Content:            action,
		ReplyCtx:           rc,
		IsCardAction:       true,
		IsQuestionResponse: true,
		QuestionResponse:   ack,
	})
	return p.awaitQuestionCardAck(ack, rc, values, questionCardAckTimeout)
}

func (p *Platform) awaitQuestionCardAck(ack <-chan core.QuestionResponseResult, rc replyContext, values map[string]any, timeout time.Duration) *callback.CardActionTriggerResponse {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-ack:
		if !result.Accepted {
			message := result.Message
			if message == "" {
				message = questionCardCopy(values, "question_unavailable", core.MsgQuestionExpired)
			}
			return questionCardToast("warning", message)
		}
		return &callback.CardActionTriggerResponse{Card: &callback.Card{
			Type: "raw", Data: p.renderBoundCardMap(questionAnswerCard(result), rc.sessionKey, rc.directUserID),
		}}
	case <-timer.C:
		// Do not hold Feishu's callback open while the engine is busy. A late
		// acceptance updates only the originating message, never a newer card.
		go p.updateQuestionCardAfterAck(ack, rc)
		return questionCardToast("info", questionCardCopy(values, "question_processing", core.MsgQuestionProcessing))
	}
}

func (p *Platform) updateQuestionCardAfterAck(ack <-chan core.QuestionResponseResult, rc replyContext) {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case result := <-ack:
		if !result.Accepted || rc.messageID == "" {
			return
		}
		if updater, ok := p.dispatchPlatform().(core.CardActionUpdater); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := updater.UpdateCard(ctx, rc, questionAnswerCard(result)); err != nil {
				slog.Warn(p.tag()+": update accepted question card failed", "error", err)
			}
		}
	case <-timer.C:
	}
}

func questionAnswerCard(result core.QuestionResponseResult) *core.Card {
	return core.NewCard().Title("✅ "+result.Answer, "green").
		Markdown(result.Question).Markdown("**→ " + result.Answer + "**").Build()
}

func questionCardToast(kind, content string) *callback.CardActionTriggerResponse {
	return &callback.CardActionTriggerResponse{Toast: &callback.Toast{Type: kind, Content: content}}
}

func questionCardCopy(values map[string]any, key string, fallback core.MsgKey) string {
	if value, ok := values[key].(string); ok && value != "" {
		return value
	}
	return core.NewI18n(core.LangEnglish).T(fallback)
}
