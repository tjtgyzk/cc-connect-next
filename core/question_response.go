package core

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
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
	callback := isQuestionResponse(msg, content)
	if !callback && strings.TrimSpace(content) == "" {
		return false
	}

	state.mu.Lock()
	if state.pending != pending || requestDone(pending.PermissionDone) || state.stopped {
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
	if more {
		pending.CurrentQuestion = idx + 1
	} else {
		updatedInput = buildAskQuestionResponse(pending.ToolInput, pending.Questions, pending.Answers)
		state.pending = nil
	}
	state.mu.Unlock()

	if !more {
		defer pending.resolve()
		if agentSession == nil {
			e.rejectQuestionResponse(msg)
			return true
		}
		if err := agentSession.RespondPermission(pending.RequestID, PermissionResult{Behavior: "allow", UpdatedInput: updatedInput}); err != nil {
			slog.Error("failed to send question response", "error", err)
			e.rejectQuestionResponse(msg)
			if msg.QuestionResponse == nil {
				e.reply(p, msg.ReplyCtx, e.i18n.T(MsgQuestionExpired))
			}
			return true
		}
	}
	acknowledgeQuestion(msg, QuestionResponseResult{Accepted: true, Question: q.Question, Answer: answer})
	// A callback-aware platform updates the original card after acknowledgement.
	if msg.QuestionResponse == nil {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgQuestionAnswered), q.Question, answer))
	}
	if more {
		e.sendAskQuestionPrompt(p, msg.ReplyCtx, pending.Questions, idx+1, pending.QuestionToken)
	}
	return true
}
