package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// turnRecordRecorder models the platform's optional TurnRecorder capability.
type turnRecordRecorder struct {
	mu      sync.Mutex
	enabled bool
	err     error
	records []TurnRecord
}

func (r *turnRecordRecorder) TurnRecordingEnabled() bool { return r.enabled }
func (r *turnRecordRecorder) RecordTurn(_ context.Context, rec TurnRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return r.err
}
func (r *turnRecordRecorder) snapshot() []TurnRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]TurnRecord(nil), r.records...)
}

type turnRecordPlatform struct {
	*stubPlatformEngine
	*turnRecordRecorder
	sendError error
}

type turnRecordCardPlatform struct {
	*stubCardPlatform
	*turnRecordRecorder
}

func (p *turnRecordPlatform) Send(ctx context.Context, replyCtx any, content string) error {
	if p.sendError != nil {
		return p.sendError
	}
	return p.stubPlatformEngine.Send(ctx, replyCtx, content)
}

func newTurnRecordPlatform() *turnRecordPlatform {
	base := &stubPlatformEngine{n: "test"}
	return &turnRecordPlatform{stubPlatformEngine: base, turnRecordRecorder: &turnRecordRecorder{enabled: true}}
}

func TestTurnRecording_OnlySuccessfulVisibleTerminalAnswers(t *testing.T) {
	for _, tc := range []struct {
		name             string
		event            Event
		enabled, stopped bool
		missingInput     bool
		mismatchedInput  bool
		recalled         bool
		deliveryError    error
		recordError      error
		wantRecords      int
	}{
		{name: "answer", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, wantRecords: 1},
		{name: "disabled", event: Event{Type: EventResult, Content: "finished answer", Done: true}},
		{name: "silent", event: Event{Type: EventResult, Content: "NO_REPLY", Done: true}, enabled: true},
		{name: "agent error", event: Event{Type: EventResult, Content: "failed answer", Error: errors.New("agent failure"), Done: true}, enabled: true},
		{name: "error event", event: Event{Type: EventError, Content: "agent failed", Error: errors.New("agent failure")}, enabled: true},
		{name: "cancelled", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, stopped: true},
		{name: "recalled", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, recalled: true},
		{name: "missing input", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, missingInput: true},
		{name: "mismatched trigger", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, mismatchedInput: true},
		{name: "delivery failed", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, deliveryError: errors.New("platform unavailable")},
		{name: "record failure preserves answer", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, recordError: errors.New("bitable unavailable"), wantRecords: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTurnRecordPlatform()
			p.enabled, p.err = tc.enabled, tc.recordError
			p.sendError = tc.deliveryError
			e := NewEngine("project", &stubAgent{}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
			t.Cleanup(func() { _ = e.Stop() })
			key := "test:group:topic"
			session := e.sessions.GetOrCreateActive(key)
			agentSession := newControllableSession("agent-turn")
			state := &interactiveState{
				agentSession:      agentSession,
				platform:          p,
				replyCtx:          "reply",
				currentUserID:     "different_runtime_credential",
				currentSessionKey: key,
				currentTurnRecord: turnRecordInput{
					userID: "ou_recipient", userName: "Alice", chatName: "Test Group",
					question: "how do I do X?", sessionKey: key, messageID: "trigger-message",
				},
			}
			if tc.missingInput {
				state.currentTurnRecord = turnRecordInput{}
			}
			if tc.mismatchedInput {
				state.currentTurnRecord.messageID = "different-trigger"
			}
			if tc.recalled {
				state.discardTurnMessageID = "trigger-message"
			}
			e.interactiveStates[key] = state
			if tc.stopped {
				state.markStopped()
			}
			agentSession.events <- tc.event
			e.processInteractiveEvents(state, session, e.sessions, key, "trigger-message", time.Now(), nil, nil, state.replyCtx)

			records := p.snapshot()
			if len(records) != tc.wantRecords {
				t.Fatalf("records = %+v, want %d", records, tc.wantRecords)
			}
			if tc.wantRecords == 0 {
				return
			}
			rec := records[0]
			if rec.UserID != "ou_recipient" || rec.UserName != "Alice" || rec.ChatName != "Test Group" {
				t.Fatalf("identity fields wrong: %+v", rec)
			}
			if rec.Question != "how do I do X?" || rec.Answer != "finished answer" {
				t.Fatalf("content fields wrong: %+v", rec)
			}
			if rec.MessageID != "trigger-message" || rec.SessionKey != key {
				t.Fatalf("routing fields wrong: %+v", rec)
			}
			if rec.At.IsZero() {
				t.Fatalf("timestamp not set: %+v", rec)
			}
		})
	}
}

// Admission, normalization, queue drain and terminal delivery are real; only
// the external agent process and platform boundary are replaced by test doubles.
func TestTurnRecording_QueuedTurnKeepsRawQuestionAndSender(t *testing.T) {
	p := newTurnRecordPlatform()
	sess := newQueuingSession("record-queue")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	e.AddAlias("short-question", "expanded agent instruction")
	key := "test:shared-group"
	startedBefore := time.Now()
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "message-A", UserID: "alice", UserName: "Alice", ChatName: "Group A", Content: "  Question A  ", ExtraContent: "quote A", ReplyCtx: "ctx-A"})
	waitForSteer(t, "first native send", func() bool { return turnRecordingSendCount(sess) == 1 })
	firstSendObserved := time.Now()
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "message-B", UserID: "bob", UserName: "Bob", ChatName: "Group B", Content: "short-question", ExtraContent: "quote B", ReplyCtx: "ctx-B"})
	sess.events <- Event{Type: EventToolUse, ToolName: "query-A"}
	sess.events <- Event{Type: EventResult, Content: "Answer A", SessionID: "native-thread", TurnID: "native-turn-A", Done: true}
	waitForSteer(t, "queued native send", func() bool { return turnRecordingSendCount(sess) == 2 })
	secondSendObserved := time.Now()
	sess.events <- Event{Type: EventToolUse, ToolName: "query-B1"}
	sess.events <- Event{Type: EventToolUse, ToolName: "query-B2"}
	sess.events <- Event{Type: EventResult, Content: "Answer B", SessionID: "native-thread", TurnID: "native-turn-B", Done: true}
	waitForSteer(t, "queued completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
	rows := p.snapshot()
	if len(rows) != 2 {
		t.Fatalf("expected exactly two delivered rows, got %+v", rows)
	}
	assertTurnRecordInput(t, rows[0], key, "message-A", "alice", "Alice", "Group A", "  Question A  ", "Answer A")
	assertTurnRecordInput(t, rows[1], key, "message-B", "bob", "Bob", "Group B", "short-question", "Answer B")
	assertTurnRecordMetadata(t, rows[0], "native-thread", "native-turn-A", 1, startedBefore, firstSendObserved)
	// B starts only after A is delivered; its earlier time in the queue is not
	// charged to B, and A's tool counter/turn identity cannot leak into B.
	assertTurnRecordMetadata(t, rows[1], "native-thread", "native-turn-B", 2, rows[0].At, secondSendObserved)
	sess.sendMu.Lock()
	queuedPrompt := sess.sendCalls[1]
	sess.sendMu.Unlock()
	if !strings.Contains(queuedPrompt, "quote B\nexpanded agent instruction") {
		t.Fatalf("fixture did not exercise prompt enrichment: %q", queuedPrompt)
	}
}

func TestTurnRecording_PendingDrainKeepsRawQuestionAndSender(t *testing.T) {
	p := newTurnRecordPlatform()
	sess := newQueuingSession("record-pending-drain")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	key := "test:pending-group"
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("could not lock idle session")
	}
	state := &interactiveState{agentSession: sess, platform: p, currentTurnRecord: turnRecordInput{userID: "old", messageID: "old", question: "old"}}
	e.interactiveStates[key] = state
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "queued", UserID: "bob", UserName: "Bob", ChatName: "Queue", Content: "raw question", ExtraContent: "quote", ReplyCtx: "ctx"})
	done := make(chan bool, 1)
	go func() { done <- e.drainPendingMessages(state, session, e.sessions, key) }()
	waitForSteer(t, "pending drain send", func() bool { return turnRecordingSendCount(sess) == 1 })
	sess.events <- Event{Type: EventResult, Content: "Answer B", Done: true}
	select {
	case unlocked := <-done:
		if !unlocked {
			t.Fatal("drain did not release session")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending drain did not complete")
	}
	rows := p.snapshot()
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %+v", rows)
	}
	assertTurnRecordInput(t, rows[0], key, "queued", "bob", "Bob", "Queue", "raw question", "Answer B")
}

func TestTurnRecording_SteerAdoptsLatestRawQuestionAndSender(t *testing.T) {
	for _, content := range []string{"  updated question  ", "/ps updated question"} {
		t.Run(strings.TrimSpace(content), func(t *testing.T) {
			p := newTurnRecordPlatform()
			sess := newSteerableSession("record-steer")
			e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
			t.Cleanup(func() { _ = e.Stop() })
			e.SetBusyMessageMode(BusyMessageModeSteer)
			key := "test:steer-group"
			startedBefore := time.Now()
			e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "message-A", UserID: "alice", UserName: "Alice", ChatName: "Group A", Content: "Question A", ReplyCtx: "ctx-A"})
			waitForSteer(t, "initial native send", func() bool { return len(sess.getSendCalls()) == 1 })
			firstSendObserved := time.Now()
			sess.events <- Event{Type: EventToolUse, ToolName: "before-steer"}
			e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "message-B", UserID: "bob", UserName: "Bob", ChatName: "Group B", Content: content, ExtraContent: "quoted context", ReplyCtx: "ctx-B"})
			if len(sess.getSteerCalls()) != 1 {
				t.Fatalf("expected one native steer: %v", sess.getSteerCalls())
			}
			sess.events <- Event{Type: EventToolUse, ToolName: "after-steer"}
			sess.events <- Event{Type: EventResult, Content: "Answer B", SessionID: "native-steer-thread", TurnID: "native-original-turn", Done: true}
			waitForSteer(t, "steered completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
			rows := p.snapshot()
			if len(rows) != 1 {
				t.Fatalf("one native turn must create one row: %+v", rows)
			}
			assertTurnRecordInput(t, rows[0], key, "message-B", "bob", "Bob", "Group B", content, "Answer B")
			assertTurnRecordMetadata(t, rows[0], "native-steer-thread", "native-original-turn", 2, startedBefore, firstSendObserved)
		})
	}
}

func TestTurnRecording_AttachmentOnlyTurnIsRecorded(t *testing.T) {
	for _, lang := range []struct {
		name string
		lang Language
		want string
	}{
		{"en", LangEnglish, "Attachment task"},
		{"zh", LangChinese, "附件任务"},
		{"zh-TW", LangTraditionalChinese, "附件任務"},
		{"ja", LangJapanese, "添付ファイルのタスク"},
		{"es", LangSpanish, "Tarea con archivos adjuntos"},
		{"auto", LangAuto, "Attachment task"},
	} {
		for _, kind := range []string{"image", "file"} {
			t.Run(lang.name+"/"+kind, func(t *testing.T) {
				p := newTurnRecordPlatform()
				sess := newQueuingSession("record-attachment")
				e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), lang.lang)
				t.Cleanup(func() { _ = e.Stop() })
				key := "test:attachment"
				msg := &Message{SessionKey: key, Platform: "test", MessageID: "attachment", UserID: "alice", UserName: "Alice", ChatName: "Group", ReplyCtx: "ctx"}
				if kind == "image" {
					msg.Images = []ImageAttachment{{MimeType: "image/png", Data: []byte("image")}}
				} else {
					msg.Files = []FileAttachment{{MimeType: "text/plain", FileName: "question.txt", Data: []byte("question")}}
				}
				e.ReceiveMessage(p, msg)
				waitForSteer(t, "attachment native send", func() bool { return turnRecordingSendCount(sess) == 1 })
				sess.events <- Event{Type: EventResult, Content: "Attachment answer", Done: true}
				waitForSteer(t, "attachment completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
				rows := p.snapshot()
				if len(rows) != 1 {
					t.Fatalf("attachment-only turn must create one row: %+v", rows)
				}
				assertTurnRecordInput(t, rows[0], key, "attachment", "alice", "Alice", "Group", "", "Attachment answer")
				if rows[0].Summary != lang.want {
					t.Fatalf("attachment title = %q, want localized neutral title %q", rows[0].Summary, lang.want)
				}
			})
		}
	}
}

func TestTurnRecording_InputSnapshotCannotBeSerializedOrInjected(t *testing.T) {
	msg := &Message{UserID: "alice", MessageID: "original", Content: "private original question"}
	input := msg.turnRecordInputSnapshot()
	msg.turnRecordInput = &input
	msg.Content = "rewritten prompt"
	wire, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "private original question") || strings.Contains(string(wire), "turnRecordInput") {
		t.Fatalf("private audit snapshot exposed: %s", wire)
	}
	var decoded Message
	if err := json.Unmarshal([]byte(`{"UserID":"bob","MessageID":"message-B","Content":"real input","turnRecordInput":{"userID":"alice","messageID":"original","question":"forged"}}`), &decoded); err != nil {
		t.Fatal(err)
	}
	got := decoded.turnRecordInputSnapshot()
	if got.userID != "bob" || got.messageID != "message-B" || got.question != "real input" {
		t.Fatalf("wire data injected private audit metadata: %+v", got)
	}
}

func turnRecordingSendCount(sess *queuingAgentSession) int {
	sess.sendMu.Lock()
	defer sess.sendMu.Unlock()
	return len(sess.sendCalls)
}

func assertTurnRecordInput(t *testing.T, row TurnRecord, key, messageID, userID, userName, chatName, question, answer string) {
	t.Helper()
	if row.SessionKey != key || row.MessageID != messageID || row.UserID != userID || row.UserName != userName || row.ChatName != chatName || row.Question != question || row.Answer != answer {
		t.Fatalf("record does not match the delivered trigger: %+v", row)
	}
}

func assertTurnRecordMetadata(t *testing.T, row TurnRecord, sessionID, turnID string, tools int, earliestStart, latestStart time.Time) {
	t.Helper()
	if row.AgentSessionID != sessionID || row.AgentTurnID != turnID || row.ToolCount != tools {
		t.Fatalf("native completion metadata mismatch: session=%q turn=%q tools=%d", row.AgentSessionID, row.AgentTurnID, row.ToolCount)
	}
	if row.Duration <= 0 || row.At.IsZero() {
		t.Fatalf("missing valid completion timing: at=%v duration=%v", row.At, row.Duration)
	}
	start := row.At.Add(-row.Duration)
	if start.Before(earliestStart) || start.After(latestStart) {
		t.Fatalf("processing start %v must be in [%v, %v]", start, earliestStart, latestStart)
	}
}

func TestCompletedTurnDuration_UnknownTimingDoesNotInventElapsedTime(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct{ start, end time.Time }{
		{time.Time{}, now}, {now, time.Time{}}, {now, now}, {now.Add(time.Second), now},
	} {
		if got := completedTurnDuration(tc.start, tc.end); got != 0 {
			t.Fatalf("unknown/invalid bounds must stay unknown, got %v", got)
		}
	}
	if got := completedTurnDuration(now, now.Add(3*time.Second)); got != 3*time.Second {
		t.Fatalf("duration = %v", got)
	}
}

func TestTurnRecording_DeliveryTimeExcludesCommitHooksAndPreservesFullContent(t *testing.T) {
	p := newTurnRecordPlatform()
	sess := newQueuingSession("runtime-session-is-not-the-terminal-id")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	hookEntered := make(chan time.Time, 1)
	releaseHook := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hookEntered <- time.Now()
		<-releaseHook
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseHook) }) })
	synchronous := false
	e.hooks = NewHookManager("test", []HookConfig{{Event: string(HookEventMessageSent), Type: string(HookHandlerHTTP), URL: server.URL, Async: &synchronous}}, "", "", "")
	question := "  " + strings.Repeat("完整原始问题\n", 300) + "  "
	answer := strings.TrimSuffix(strings.Repeat("完整回答，不能被任务标题截断。\n", 300), "\n")
	key := "test:full-content"
	startedBefore := time.Now()
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "full-message", UserID: "alice", Content: question, ReplyCtx: "ctx"})
	waitForSteer(t, "long answer native send", func() bool { return turnRecordingSendCount(sess) == 1 })
	sendObserved := time.Now()
	sess.events <- Event{Type: EventResult, Content: answer, SessionID: "terminal-native-thread", TurnID: "terminal-native-turn", Done: true}
	var hookTime time.Time
	select {
	case hookTime = <-hookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit hook was not reached")
	}
	if len(p.snapshot()) != 0 {
		t.Fatal("recording should occur after commit, with delivery time already frozen")
	}
	releaseOnce.Do(func() { close(releaseHook) })
	waitForSteer(t, "long answer completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
	rows := p.snapshot()
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	row := rows[0]
	if row.Question != question || row.Answer != answer {
		t.Fatalf("full question/answer changed: lengths question=%d/%d answer=%d/%d", len(row.Question), len(question), len(row.Answer), len(answer))
	}
	if len([]rune(row.Summary)) != turnRecordSummaryLimit || !strings.HasSuffix(row.Summary, "…") || strings.Contains(row.Summary, "\n") {
		t.Fatalf("bounded Unicode task title = %q", row.Summary)
	}
	if row.At.After(hookTime) {
		t.Fatalf("post-delivery commit time leaked into At/Duration: at=%v hook=%v", row.At, hookTime)
	}
	assertTurnRecordMetadata(t, row, "terminal-native-thread", "terminal-native-turn", 0, startedBefore, sendObserved)
}

type turnRecordBlockingTypingPlatform struct {
	*turnRecordPlatform
	typingEntered chan time.Time
	releaseTyping chan struct{}
}

func (p *turnRecordBlockingTypingPlatform) StartTyping(ctx context.Context, _ any) func() {
	p.typingEntered <- time.Now()
	select {
	case <-p.releaseTyping:
	case <-ctx.Done():
	}
	return func() {}
}

func TestTurnRecording_PendingDrainStartsTimingBeforeNativeSendAndTyping(t *testing.T) {
	p := &turnRecordBlockingTypingPlatform{turnRecordPlatform: newTurnRecordPlatform(), typingEntered: make(chan time.Time, 1), releaseTyping: make(chan struct{})}
	sess := newQueuingSession("drain-runtime-session")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(p.releaseTyping) }) })
	key := "test:drain-timing"
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("could not lock session")
	}
	state := &interactiveState{agentSession: sess, platform: p}
	e.interactiveStates[key] = state
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "drained-message", UserID: "alice", Content: "queued question", ReplyCtx: "ctx"})
	startedBefore := time.Now()
	done := make(chan bool, 1)
	go func() { done <- e.drainPendingMessages(state, session, e.sessions, key) }()
	var typingTime time.Time
	select {
	case typingTime = <-p.typingEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("typing setup was not reached")
	}
	waitForSteer(t, "drained native send", func() bool { return turnRecordingSendCount(sess) == 1 })
	sess.events <- Event{Type: EventResult, Content: "done", SessionID: "drained-native-thread", TurnID: "drained-native-turn", Done: true}
	releaseOnce.Do(func() { close(p.releaseTyping) })
	select {
	case unlocked := <-done:
		if !unlocked {
			t.Fatal("drain did not unlock")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not complete")
	}
	rows := p.snapshot()
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	assertTurnRecordMetadata(t, rows[0], "drained-native-thread", "drained-native-turn", 0, startedBefore, typingTime)
}

func TestTurnRecording_HITLWaitStaysInOriginalNativeTurn(t *testing.T) {
	p := newTurnRecordPlatform()
	sess := newQueuingSession("hitl-runtime-session")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	key := "test:hitl-timing"
	startedBefore := time.Now()
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "original-question", UserID: "alice", Content: "check service", ReplyCtx: "ctx"})
	waitForSteer(t, "HITL native send", func() bool { return turnRecordingSendCount(sess) == 1 })
	sendObserved := time.Now()
	sess.events <- Event{Type: EventToolUse, ToolName: "request_user_input"}
	sess.events <- Event{Type: EventPermissionRequest, RequestID: "environment-request", ToolName: "AskUserQuestion", Questions: []UserQuestion{{ID: "environment", Question: "prod or stg?", Options: []UserQuestionOption{{Label: "prod"}, {Label: "stg"}}}}}
	var state *interactiveState
	var action string
	waitForSteer(t, "HITL pending question", func() bool {
		e.interactiveMu.Lock()
		state = e.interactiveStates[key]
		e.interactiveMu.Unlock()
		if state == nil {
			return false
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.pending == nil {
			return false
		}
		action = fmt.Sprintf("askq:%s:0:2", state.pending.QuestionToken)
		return true
	})
	if len(p.snapshot()) != 0 || !e.sessions.GetOrCreateActive(key).Busy() {
		t.Fatal("waiting for input must leave the original turn running and unarchived")
	}
	answerAt := time.Now()
	message := questionTestMessage(key, "alice", action)
	e.ReceiveMessage(p, message)
	select {
	case ack := <-message.QuestionResponse:
		if !ack.Accepted {
			t.Fatal("native answer was not accepted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native answer was not acknowledged")
	}
	sess.events <- Event{Type: EventToolUse, ToolName: "query-after-answer"}
	sess.events <- Event{Type: EventResult, Content: "stg checked", SessionID: "hitl-native-thread", TurnID: "same-native-turn", Done: true}
	waitForSteer(t, "HITL final completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
	rows := p.snapshot()
	if len(rows) != 1 || turnRecordingSendCount(sess) != 1 {
		t.Fatalf("HITL must continue one turn: records=%d sends=%d", len(rows), turnRecordingSendCount(sess))
	}
	assertTurnRecordInput(t, rows[0], key, "original-question", "alice", "", "", "check service", "stg checked")
	assertTurnRecordMetadata(t, rows[0], "hitl-native-thread", "same-native-turn", 2, startedBefore, sendObserved)
	if rows[0].At.Before(answerAt) || rows[0].Duration < rows[0].At.Sub(answerAt) {
		t.Fatal("HITL waiting interval was excluded from the original turn duration")
	}
}
