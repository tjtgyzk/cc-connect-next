package core

import (
	"context"
	"log/slog"
	"time"
)

// turnRecordInput travels with the trigger message through queueing and steer
// adoption. Runtime authorization can change independently of presentation
// ownership, so audit identity must not be read back from active credentials.
type turnRecordInput struct {
	userID     string
	userName   string
	chatName   string
	sessionKey string
	messageID  string
	question   string
}

func (msg *Message) turnRecordInputSnapshot() turnRecordInput {
	if msg.turnRecordInput != nil {
		return *msg.turnRecordInput
	}
	return turnRecordInput{
		userID: msg.UserID, userName: msg.UserName, chatName: msg.ChatName,
		sessionKey: msg.SessionKey, messageID: msg.MessageID, question: msg.Content,
	}
}

// recordTurn persists a successfully delivered turn through the platform's
// optional TurnRecorder capability. It is an
// auxiliary side channel and must never fail, delay, or re-run the user's turn.
func (completion *turnCompletion) recordTurn(response preparedTurnResponse) {
	if response.isSilent || completion.event.Error != nil {
		return
	}
	p := completion.platform
	recorder, ok := p.(TurnRecorder)
	if !ok || !recorder.TurnRecordingEnabled() {
		return
	}
	t := completion.processor
	state := t.state

	state.mu.Lock()
	input := state.currentTurnRecord
	state.mu.Unlock()

	// An empty question is valid for image/file-only inputs. Require the
	// matching trigger instead of inferring whether an audit input exists
	// from its text, and never attach a successor's identity to an old result.
	if input.userID == "" || completion.msgID == "" || input.messageID != completion.msgID ||
		state.isStopped() || state.shouldDiscardTurn(completion.msgID) {
		return
	}

	record := TurnRecord{
		At:         time.Now(),
		UserID:     input.userID,
		UserName:   input.userName,
		ChatName:   input.chatName,
		Model:      t.engine.turnModel(state),
		SessionKey: input.sessionKey,
		Question:   input.question,
		Answer:     response.base,
		MessageID:  completion.msgID,
	}

	ctx, cancel := context.WithTimeout(completion.terminalDeliveryCtx, 15*time.Second)
	defer cancel()
	if err := recorder.RecordTurn(ctx, record); err != nil {
		slog.Warn("turn recording unavailable", "platform", p.Name(), "error", err)
	}
}

// turnModel reports the model in use for the current turn, when the backend
// exposes it. It is side-effect-free and returns "" when unavailable.
func (e *Engine) turnModel(state *interactiveState) string {
	if state == nil {
		return ""
	}
	state.mu.Lock()
	agentSession := state.agentSession
	agent := state.agent
	state.mu.Unlock()
	if agent == nil {
		agent = e.agent
	}
	return replyFooterModel(agentSession, agent)
}
