package core

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strings"
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

// A steer continues the original native turn. Keep all accepted raw questions
// while retaining the original archive requester. Delivery ownership still
// follows the latest trigger. Prompt-enriched content (capability briefs,
// quotes, sender metadata) never enters the archive.
func (e *Engine) appendSteeredTurnRecord(previous, next turnRecordInput) turnRecordInput {
	if previous.userID == "" || previous.messageID == "" {
		return next
	}
	sender := strings.TrimSpace(next.userName)
	if sender == "" {
		sender = next.userID
	}
	next.userID, next.userName = previous.userID, previous.userName
	next.chatName, next.sessionKey = previous.chatName, previous.sessionKey
	if previous.messageID == next.messageID || next.question == "" {
		next.question = previous.question
		return next
	}
	addition := e.i18n.Tf(MsgTurnRecordFollowup, sender) + "\n" + next.question
	if previous.question == "" {
		next.question = addition
	} else {
		next.question = previous.question + "\n\n" + addition
	}
	return next
}

// Derive identity from the exact local session that processed the turn, never
// from the current active session or a replaceable provider/native thread ID.
// CreatedAt prevents collisions if a session store is recreated from s1.
func (e *Engine) turnConversationID(session *Session, sessionKey string) string {
	if session == nil {
		return ""
	}
	session.mu.Lock()
	id, createdAt := session.ID, session.CreatedAt
	session.mu.Unlock()
	if id == "" {
		return ""
	}
	identity := fmt.Sprintf("%q:%q:%q:%q", e.name, sessionKey, id, createdAt.UTC().Format(time.RFC3339Nano))
	return fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
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
	summary := turnRecordSummary(input.question)
	if summary == "" {
		summary = t.engine.i18n.T(MsgTurnRecordAttachmentTask)
	}

	record := TurnRecord{
		At:             completion.deliveredAt,
		UserID:         input.userID,
		UserName:       input.userName,
		ChatName:       input.chatName,
		Model:          t.engine.turnModel(state),
		SessionKey:     input.sessionKey,
		ConversationID: t.engine.turnConversationID(t.session, input.sessionKey),
		Language:       t.engine.i18n.CurrentLang(),
		Question:       input.question,
		Answer:         response.base,
		MessageID:      completion.msgID,
		Summary:        summary,
		Duration:       completedTurnDuration(completion.turnStart, completion.deliveredAt),
		ToolCount:      completion.toolCount,
		AgentSessionID: completion.event.SessionID,
		AgentTurnID:    completion.event.TurnID,
	}

	ctx, cancel := context.WithTimeout(completion.terminalDeliveryCtx, 15*time.Second)
	defer cancel()
	if err := recorder.RecordTurn(ctx, record); err != nil {
		slog.Warn("turn recording unavailable", "platform", p.Name(), "error", err)
	}
}

const turnRecordSummaryLimit = 80

func turnRecordSummary(question string) string {
	title := strings.Join(strings.Fields(question), " ")
	if title == "" {
		return ""
	}
	runes := []rune(title)
	if len(runes) > turnRecordSummaryLimit {
		return string(runes[:turnRecordSummaryLimit-1]) + "…"
	}
	return title
}

func completedTurnDuration(start, delivered time.Time) time.Duration {
	if start.IsZero() || delivered.IsZero() || !delivered.After(start) {
		return 0
	}
	return delivered.Sub(start)
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
