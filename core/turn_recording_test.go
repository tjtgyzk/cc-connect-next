package core

import (
	"context"
	"encoding/json"
	"errors"
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
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "message-A", UserID: "alice", UserName: "Alice", ChatName: "Group A", Content: "  Question A  ", ExtraContent: "quote A", ReplyCtx: "ctx-A"})
	waitForSteer(t, "first native send", func() bool { return turnRecordingSendCount(sess) == 1 })
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "message-B", UserID: "bob", UserName: "Bob", ChatName: "Group B", Content: "short-question", ExtraContent: "quote B", ReplyCtx: "ctx-B"})
	sess.events <- Event{Type: EventResult, Content: "Answer A", Done: true}
	waitForSteer(t, "queued native send", func() bool { return turnRecordingSendCount(sess) == 2 })
	sess.events <- Event{Type: EventResult, Content: "Answer B", Done: true}
	waitForSteer(t, "queued completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
	rows := p.snapshot()
	if len(rows) != 2 {
		t.Fatalf("expected exactly two delivered rows, got %+v", rows)
	}
	assertTurnRecordInput(t, rows[0], key, "message-A", "alice", "Alice", "Group A", "  Question A  ", "Answer A")
	assertTurnRecordInput(t, rows[1], key, "message-B", "bob", "Bob", "Group B", "short-question", "Answer B")
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
			e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "message-A", UserID: "alice", UserName: "Alice", ChatName: "Group A", Content: "Question A", ReplyCtx: "ctx-A"})
			waitForSteer(t, "initial native send", func() bool { return len(sess.getSendCalls()) == 1 })
			e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "message-B", UserID: "bob", UserName: "Bob", ChatName: "Group B", Content: content, ExtraContent: "quoted context", ReplyCtx: "ctx-B"})
			if len(sess.getSteerCalls()) != 1 {
				t.Fatalf("expected one native steer: %v", sess.getSteerCalls())
			}
			sess.events <- Event{Type: EventResult, Content: "Answer B", Done: true}
			waitForSteer(t, "steered completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
			rows := p.snapshot()
			if len(rows) != 1 {
				t.Fatalf("one native turn must create one row: %+v", rows)
			}
			assertTurnRecordInput(t, rows[0], key, "message-B", "bob", "Bob", "Group B", content, "Answer B")
		})
	}
}

func TestTurnRecording_AttachmentOnlyTurnIsRecorded(t *testing.T) {
	for _, kind := range []string{"image", "file"} {
		t.Run(kind, func(t *testing.T) {
			p := newTurnRecordPlatform()
			sess := newQueuingSession("record-attachment")
			e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
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
		})
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
