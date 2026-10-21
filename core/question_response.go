package core

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

func isQuestionResponse(msg *Message, content string) bool {
	return msg.IsQuestionResponse || strings.HasPrefix(strings.TrimSpace(content), "askq:")
}

func questionTokenMatches(pending *pendingPermission, content string) bool {
	parts := strings.Split(strings.TrimSpace(content), ":")
	return pending != nil && pending.QuestionToken != "" && len(parts) == 4 && parts[0] == "askq" && parts[1] == pending.QuestionToken
}

func requestDone(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func acknowledgeQuestion(msg *Message, result QuestionResponseResult) {
	send := func() {
		if msg.QuestionResponse != nil {
			select {
			case msg.QuestionResponse <- result:
			default:
			}
		}
	}
	if msg.questionResponseOnce != nil {
		msg.questionResponseOnce.Do(send)
	} else {
		send()
	}
}

func (e *Engine) rejectQuestionResponse(msg *Message) {
	acknowledgeQuestion(msg, QuestionResponseResult{Message: e.i18n.T(MsgQuestionExpired)})
}

// handleQuestionResponse claims a question exactly once under state.mu. No
// platform or agent call is made while holding the state lock.
func (e *Engine) handleQuestionResponse(p Platform, msg *Message, content string, state *interactiveState, pending *pendingPermission) bool {
	receivedAt := time.Now()
	callback := isQuestionResponse(msg, content)
	if !callback && strings.TrimSpace(content) == "" {
		return false
	}

	state.mu.Lock()
	if pending.responseClaimed {
		state.mu.Unlock()
		if !callback {
			return false // a follow-up during the native write keeps its normal queue/steer path
		}
		e.rejectQuestionResponse(msg)
		return true
	}
	if state.pending != pending || !state.permissionBindingValidLocked(pending) || requestDone(pending.PermissionDone) || state.stopped {
		if state.pending == pending {
			state.pending = nil
			pending.resolve()
		}
		state.mu.Unlock()
		e.rejectQuestionResponse(msg)
		return true
	}
	idx := pending.CurrentQuestion
	if idx < 0 || idx >= len(pending.Questions) || (pending.QuestionUserID != "" && pending.QuestionUserID != msg.UserID) {
		state.mu.Unlock()
		e.rejectQuestionResponse(msg)
		return true
	}
	q := pending.Questions[idx]
	var answer string
	if callback {
		parts := strings.Split(strings.TrimSpace(content), ":")
		if !questionTokenMatches(pending, content) || q.MultiSelect {
			state.mu.Unlock()
			e.rejectQuestionResponse(msg)
			return true
		}
		qIdx, qErr := strconv.Atoi(parts[2])
		optIdx, optErr := strconv.Atoi(parts[3])
		if qErr != nil || optErr != nil || qIdx != idx || optIdx < 1 || optIdx > len(q.Options) {
			state.mu.Unlock()
			e.rejectQuestionResponse(msg)
			return true
		}
		answer = q.Options[optIdx-1].Label
	} else {
		answer = e.resolveAskQuestionAnswer(q, content)
	}
	if strings.TrimSpace(answer) == "" {
		state.mu.Unlock()
		e.rejectQuestionResponse(msg)
		return true
	}
	if pending.Answers == nil {
		pending.Answers = make(map[int]string)
	}
	pending.Answers[idx] = answer
	more := idx+1 < len(pending.Questions)
	var updatedInput map[string]any
	agentSession := state.agentSession
	recordInput := state.currentTurnRecord
	messageID := state.currentMessageID
	generation := state.presentationGeneration
	state.pausePermissionWaitLocked(pending, receivedAt)
	if more {
		pending.waitPromptDelivered = false
		pending.CurrentQuestion = idx + 1
	} else {
		updatedInput = buildAskQuestionResponse(pending.ToolInput, pending.Questions, pending.Answers)
		pending.responseClaimed = true
		pending.responseDone = make(chan struct{})
	}
	state.mu.Unlock()

	if !more {
		if agentSession == nil {
			state.finishPermissionResponse(pending, agentSession, false, false, time.Now())
			e.rejectQuestionResponse(msg)
			return true
		}
		if err := agentSession.RespondPermission(pending.RequestID, PermissionResult{Behavior: "allow", UpdatedInput: updatedInput}); err != nil {
			state.finishPermissionResponse(pending, agentSession, false, false, time.Now())
			slog.Error("failed to send question response", "error", err)
			e.rejectQuestionResponse(msg)
			if msg.QuestionResponse == nil {
				e.reply(p, msg.ReplyCtx, e.i18n.T(MsgQuestionExpired))
			}
			return true
		}
		acceptedAt := time.Now()
		e.recordAcceptedQuestion(state, agentSession, recordInput, messageID, generation, pending, msg)
		state.finishPermissionResponse(pending, agentSession, true, false, acceptedAt)
	}
	acknowledgeQuestion(msg, QuestionResponseResult{Accepted: true, Question: q.Question, Answer: answer})
	// A callback-aware platform updates the original card after acknowledgement.
	if msg.QuestionResponse == nil {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgQuestionAnswered), q.Question, answer))
	}
	if more {
		if err := e.sendAskQuestionPrompt(p, msg.ReplyCtx, pending.Questions, idx+1, pending.QuestionToken); err == nil {
			state.startPermissionWait(pending, time.Now())
		}
	}
	return true
}

// Record only accepted structured answers. Permission ToolInput may contain
// commands or credentials and must never be serialized into the conversation.
func (e *Engine) recordAcceptedQuestion(state *interactiveState, agentSession AgentSession, input turnRecordInput, messageID string, generation uint64, pending *pendingPermission, msg *Message) {
	if input.userID == "" || messageID == "" || input.messageID != messageID || pending.ToolName != "AskUserQuestion" || len(pending.Questions) == 0 {
		return
	}
	sender := strings.TrimSpace(msg.UserName)
	if sender == "" && msg.UserID == input.userID {
		sender = input.userName
	}
	if sender == "" {
		sender = msg.UserID
	}
	parts := []string{e.i18n.Tf(MsgTurnRecordClarification, sender)}
	for index, question := range pending.Questions {
		answer, ok := pending.Answers[index]
		if !ok || strings.TrimSpace(answer) == "" {
			return
		}
		parts = append(parts, e.i18n.Tf(MsgQuestionAnswered, question.Question, answer))
	}
	addition := strings.Join(parts, "\n")
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopped || e.ctx.Err() != nil || state.agentSession != agentSession ||
		state.currentMessageID != messageID || state.presentationGeneration != generation ||
		state.discardTurnMessageID == messageID || state.currentTurnRecord.messageID != input.messageID ||
		state.currentTurnRecord.sessionKey != input.sessionKey || state.currentTurnRecord.userID != input.userID {
		return
	}
	if state.currentTurnRecord.question == "" {
		state.currentTurnRecord.question = addition
	} else {
		state.currentTurnRecord.question += "\n\n" + addition
	}
}
