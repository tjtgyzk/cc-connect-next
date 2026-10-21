package core

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const questionTestToken = "0123456789abcdef01234567"

func questionTestState(e *Engine, key string, rec AgentSession, questions []UserQuestion) *interactiveState {
	state := &interactiveState{
		agentSession:     rec,
		currentMessageID: "original-message", currentSessionKey: key,
		currentTurnRecord: turnRecordInput{userID: "alice", userName: "Alice", sessionKey: key, messageID: "original-message", question: "original question"},
		pending: &pendingPermission{
			RequestID: "native-request-1", ToolName: "AskUserQuestion", QuestionToken: questionTestToken,
			QuestionUserID: "alice", Questions: questions, Resolved: make(chan struct{}),
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()
	return state
}

func questionTestMessage(key, user, content string) *Message {
	return &Message{SessionKey: key, UserID: user, Content: content, ReplyCtx: "ctx", IsQuestionResponse: true, QuestionResponse: make(chan QuestionResponseResult, 1)}
}

func TestQuestionCallback_RejectsUnboundStaleWrongIndexAndWrongOwner(t *testing.T) {
	for _, tc := range []struct{ name, content, user string }{
		{"legacy", "askq:0:1", "alice"},
		{"legacy-short", "askq:1", "alice"},
		{"wrong-request", "askq:oldtoken:0:1", "alice"},
		{"wrong-question", "askq:" + questionTestToken + ":1:1", "alice"},
		{"bad-question", "askq:" + questionTestToken + ":x:1", "alice"},
		{"bad-option", "askq:" + questionTestToken + ":0:0", "alice"},
		{"large-option", "askq:" + questionTestToken + ":0:100", "alice"},
		{"other-user", "askq:" + questionTestToken + ":0:1", "bob"},
		{"empty", "", "alice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, p, rec := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}
			state := questionTestState(e, "test:question", rec, testQuestions())
			msg := questionTestMessage("test:question", tc.user, tc.content)
			if !e.handlePendingPermission(p, msg, msg.Content, "") {
				t.Fatal("callback must not become a prompt")
			}
			ack := <-msg.QuestionResponse
			if ack.Accepted || ack.Message == "" {
				t.Fatalf("rejected ack = %#v", ack)
			}
			if rec.calls != 0 || state.pending == nil || len(state.pending.Answers) != 0 {
				t.Fatal("invalid callback changed pending state")
			}
			if state.currentTurnRecord.question != "original question" {
				t.Fatal("invalid callback changed the archive")
			}
		})
	}
}

func TestQuestionCallback_NoPendingNeverBecomesPrompt(t *testing.T) {
	e, p := newTestEngine(), &stubPlatformEngine{n: "test"}
	for _, content := range []string{"askq:0:1", "askq:" + questionTestToken + ":0:1"} {
		msg := questionTestMessage("test:missing", "alice", content)
		e.ReceiveMessage(p, msg)
		if ack := <-msg.QuestionResponse; ack.Accepted {
			t.Fatal("stale callback accepted")
		}
	}
	if len(e.sessions.ListSessions("test:missing")) != 0 {
		t.Fatal("stale callback created a session")
	}
}

func TestQuestionCallback_ConcurrentDuplicateConsumedOnce(t *testing.T) {
	e, p, rec := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}
	state := questionTestState(e, "test:question", rec, testQuestions())
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := questionTestMessage("test:question", "alice", "askq:"+questionTestToken+":0:2")
			e.handlePendingPermission(p, msg, msg.Content, "")
			if (<-msg.QuestionResponse).Accepted {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 || rec.calls != 1 {
		t.Fatalf("accepted=%d calls=%d", accepted.Load(), rec.calls)
	}
	if got, want := state.currentTurnRecord.question, "original question\n\nClarification (Alice):\n✅ Which database?: **SQLite**"; got != want {
		t.Fatalf("archive = %q, want %q", got, want)
	}
}

func TestQuestionCallback_MultiQuestionRejectsPreviousCard(t *testing.T) {
	e, p, rec := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}
	state := questionTestState(e, "test:question", rec, testMultiQuestions())
	first := "askq:" + questionTestToken + ":0:1"
	for i, action := range []string{first, first, "askq:" + questionTestToken + ":1:2"} {
		msg := questionTestMessage("test:question", "alice", action)
		e.handlePendingPermission(p, msg, msg.Content, "")
		ack := <-msg.QuestionResponse
		if ack.Accepted != (i != 1) {
			t.Fatalf("step %d ack=%#v", i, ack)
		}
		if i < 2 && state.currentTurnRecord.question != "original question" {
			t.Fatal("partial answers must wait for native acceptance")
		}
	}
	if rec.calls != 1 || state.pending != nil {
		t.Fatal("multi-question response not completed exactly once")
	}
	answers := rec.lastResult.UpdatedInput["answers"].(map[string]any)
	if answers["Which database?"] != "PostgreSQL" || answers["Which framework?"] != "Echo" {
		t.Fatalf("answers=%#v", answers)
	}
	if got, want := state.currentTurnRecord.question, "original question\n\nClarification (Alice):\n✅ Which database?: **PostgreSQL**\n✅ Which framework?: **Echo**"; got != want {
		t.Fatalf("archive = %q, want %q", got, want)
	}
}

func TestQuestionCallback_ClosedNativeRequestRejectsAndReleases(t *testing.T) {
	e, p, rec := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}
	state := questionTestState(e, "test:question", rec, testQuestions())
	done := make(chan struct{})
	pending := state.pending
	pending.PermissionDone = done
	close(done)
	msg := questionTestMessage("test:question", "alice", "askq:"+questionTestToken+":0:1")
	e.handlePendingPermission(p, msg, msg.Content, "")
	if ack := <-msg.QuestionResponse; ack.Accepted {
		t.Fatal("cancelled native request accepted")
	}
	if state.pending != nil || rec.calls != 0 {
		t.Fatal("cancelled request not released")
	}
	if state.currentTurnRecord.question != "original question" {
		t.Fatal("expired question changed the archive")
	}
	select {
	case <-pending.Resolved:
	default:
		t.Fatal("pending waiter not released")
	}
}

type questionErrorSession struct{ stubAgentSession }

func (*questionErrorSession) RespondPermission(string, PermissionResult) error {
	return errors.New("request no longer pending")
}

func TestQuestionCallback_AgentRejectDoesNotAcknowledgeSuccess(t *testing.T) {
	e, p := newTestEngine(), &stubPlatformEngine{n: "test"}
	state := questionTestState(e, "test:question", &questionErrorSession{}, testQuestions())
	msg := questionTestMessage("test:question", "alice", "askq:"+questionTestToken+":0:1")
	e.handlePendingPermission(p, msg, msg.Content, "")
	if ack := <-msg.QuestionResponse; ack.Accepted || ack.Message == "" {
		t.Fatalf("ack=%#v", ack)
	}
	if state.currentTurnRecord.question != "original question" {
		t.Fatal("failed native response changed the archive")
	}
}

type questionHookSession struct {
	stubAgentSession
	respond func()
}

func (s *questionHookSession) RespondPermission(string, PermissionResult) error {
	s.respond()
	return nil
}

func TestQuestionCallback_ArchiveRejectsChangedTurnBinding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*interactiveState)
	}{
		{"message", func(s *interactiveState) { s.currentMessageID = "new-message" }},
		{"generation", func(s *interactiveState) { s.presentationGeneration++ }},
		{"session", func(s *interactiveState) { s.agentSession = &stubAgentSession{} }},
		{"record message", func(s *interactiveState) { s.currentTurnRecord.messageID = "new-message" }},
		{"record session", func(s *interactiveState) { s.currentTurnRecord.sessionKey = "new-session" }},
		{"record user", func(s *interactiveState) { s.currentTurnRecord.userID = "bob" }},
		{"stopped", func(s *interactiveState) { s.stopped = true }},
		{"recalled", func(s *interactiveState) { s.discardTurnMessageID = s.currentMessageID }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, p := newTestEngine(), &stubPlatformEngine{n: "test"}
			rec := &questionHookSession{}
			state := questionTestState(e, "test:question", rec, testQuestions())
			rec.respond = func() { state.mu.Lock(); defer state.mu.Unlock(); tc.change(state) }
			msg := questionTestMessage("test:question", "alice", "askq:"+questionTestToken+":0:1")
			e.handlePendingPermission(p, msg, msg.Content, "")
			if ack := <-msg.QuestionResponse; !ack.Accepted {
				t.Fatal("native acceptance itself must be unchanged")
			}
			if state.currentTurnRecord.question != "original question" {
				t.Fatal("late callback changed a replaced or cancelled archive")
			}
		})
	}
}

func TestQuestionCallback_ArchiveIncludesOnlyStructuredQuestionAndAnswer(t *testing.T) {
	e, p, rec := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}
	state := questionTestState(e, "test:question", rec, testQuestions())
	state.pending.ToolInput = map[string]any{"command": "private-command", "token": "secret-token"}
	msg := questionTestMessage("test:question", "alice", "askq:"+questionTestToken+":0:1")
	e.handlePendingPermission(p, msg, msg.Content, "")
	if ack := <-msg.QuestionResponse; !ack.Accepted {
		t.Fatal("native answer rejected")
	}
	if strings.Contains(state.currentTurnRecord.question, "private-command") || strings.Contains(state.currentTurnRecord.question, "secret-token") {
		t.Fatal("permission metadata leaked into the archive")
	}
	state = questionTestState(e, "test:tool-permission", rec, nil)
	pending := state.pending
	pending.ToolName = "Bash"
	pending.ToolInput = map[string]any{"command": "private-command"}
	permission := &Message{SessionKey: "test:tool-permission", UserID: "alice", Content: "allow", ReplyCtx: "ctx", IsPermissionResponse: true}
	if !e.handlePendingPermission(p, permission, permission.Content, "") || rec.calls != 2 || state.pending != nil {
		t.Fatal("ordinary tool permission did not follow its existing response path")
	}
	if state.currentTurnRecord.question != "original question" {
		t.Fatal("ordinary tool permission was archived as a question")
	}
}

func TestQuestionCallback_CronFallbackMatchesToken(t *testing.T) {
	e, p, rec := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}
	wrong := questionTestState(e, "test:question", &recordingAgentSession{}, testQuestions())
	wrong.pending.QuestionToken = "other"
	questionTestState(e, "test:question#cron:one", rec, testQuestions())
	msg := questionTestMessage("test:question", "alice", "askq:"+questionTestToken+":0:1")
	e.handlePendingPermission(p, msg, msg.Content, "")
	if ack := <-msg.QuestionResponse; !ack.Accepted {
		t.Fatalf("ack=%#v", ack)
	}
	if rec.calls != 1 || wrong.pending == nil {
		t.Fatal("wrong session consumed")
	}
}

func TestQuestionResponse_UsesIDsForIdenticalQuestionText(t *testing.T) {
	qs := []UserQuestion{{ID: "one", Question: "Which environment?"}, {ID: "two", Question: "Which environment?"}}
	got := buildAskQuestionResponse(nil, qs, map[int]string{0: "prod", 1: "stg"})["answers"].(map[string]any)
	if got["one"] != "prod" || got["two"] != "stg" {
		t.Fatalf("answers=%#v", got)
	}
}

func TestQuestionCard_BoundActionsFitInlineLimit(t *testing.T) {
	e := newTestEngine()
	p := &stubInlineButtonPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e.sendAskQuestionPrompt(p, "ctx", testQuestions(), 0, questionTestToken)
	for _, row := range p.buttonRows {
		for _, button := range row {
			if len(button.Data) > 64 || !strings.HasPrefix(button.Data, "askq:"+questionTestToken+":0:") {
				t.Fatalf("invalid action %q", button.Data)
			}
		}
	}
	if got := fmt.Sprint(p.buttonRows[0][0].Data); got == "" {
		t.Fatal("missing action")
	}
}

func TestQuestionCallback_PreservesOpaqueActionAndAcknowledgesOnce(t *testing.T) {
	e, p, rec := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}
	defer e.Stop()
	questionTestState(e, "test:question", rec, testQuestions())
	action := "askq:" + questionTestToken + ":0:1"
	e.aliases[action] = "/new"
	e.SetBannedWords([]string{"askq"})
	e.SetRateLimitCfg(RateLimitCfg{MaxMessages: 1, Window: time.Hour})
	msg := questionTestMessage("test:question", "alice", action)
	if !e.checkRateLimit(msg) {
		t.Fatal("initial rate token unavailable")
	}
	e.ReceiveMessage(p, msg)
	if ack := <-msg.QuestionResponse; !ack.Accepted {
		t.Fatalf("opaque callback rejected: %#v", ack)
	}
	select {
	case second := <-msg.QuestionResponse:
		t.Fatalf("duplicate acknowledgement: %#v", second)
	default:
	}
	if rec.calls != 1 {
		t.Fatalf("native response calls=%d", rec.calls)
	}
}

func TestQuestionCallback_EarlyReturnAcknowledgesRejection(t *testing.T) {
	e, p := newTestEngine(), &stubPlatformEngine{n: "test"}
	msg := questionTestMessage("test:question", "alice", "askq:"+questionTestToken+":0:1")
	msg.Recalled = true
	e.ReceiveMessage(p, msg)
	select {
	case ack := <-msg.QuestionResponse:
		if ack.Accepted || ack.Message == "" {
			t.Fatalf("early-return ack=%#v", ack)
		}
	default:
		t.Fatal("early return left callback waiting")
	}
}
