package core

import (
	"context"
	"log/slog"
	"time"
)

// recordTurn persists a successfully delivered turn through the platform's
// optional TurnRecorder capability. It mirrors notifyCompletion: it is an
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
	userID := state.currentReplyUserID
	if userID == "" {
		userID = state.currentUserID
	}
	userName := state.currentUserName
	chatName := state.currentChatName
	sessionKey := state.currentSessionKey
	question := state.currentUserQuestion
	state.mu.Unlock()

	if userID == "" || question == "" || completion.msgID == "" ||
		state.isStopped() || state.shouldDiscardTurn(completion.msgID) {
		return
	}

	record := TurnRecord{
		At:         time.Now(),
		UserID:     userID,
		UserName:   userName,
		ChatName:   chatName,
		Model:      t.engine.turnModel(state),
		SessionKey: sessionKey,
		Question:   question,
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
	state.mu.Unlock()
	if provider, ok := agentSession.(AgentDiagnosticProvider); ok {
		return provider.DiagnosticSnapshot().Model
	}
	return ""
}
