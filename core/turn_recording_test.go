package core

import (
	"context"
	"errors"
	"path/filepath"
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
		missingQuestion  bool
		recordError      error
		wantRecords      int
	}{
		{name: "answer", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, wantRecords: 1},
		{name: "disabled", event: Event{Type: EventResult, Content: "finished answer", Done: true}},
		{name: "silent", event: Event{Type: EventResult, Content: "NO_REPLY", Done: true}, enabled: true},
		{name: "agent error", event: Event{Type: EventResult, Content: "failed answer", Error: errors.New("agent failure"), Done: true}, enabled: true},
		{name: "cancelled", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, stopped: true},
		{name: "missing question", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, missingQuestion: true},
		{name: "record failure preserves answer", event: Event{Type: EventResult, Content: "finished answer", Done: true}, enabled: true, recordError: errors.New("bitable unavailable"), wantRecords: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTurnRecordPlatform()
			p.enabled, p.err = tc.enabled, tc.recordError
			e := NewEngine("project", &stubAgent{}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
			t.Cleanup(func() { _ = e.Stop() })
			key := "test:group:topic"
			session := e.sessions.GetOrCreateActive(key)
			agentSession := newControllableSession("agent-turn")
			state := &interactiveState{
				agentSession:        agentSession,
				platform:            p,
				replyCtx:            "reply",
				currentUserID:       "ou_recipient",
				currentReplyUserID:  "ou_recipient",
				currentSessionKey:   key,
				currentUserQuestion: "how do I do X?",
				currentUserName:     "Alice",
				currentChatName:     "Test Group",
			}
			if tc.missingQuestion {
				state.currentUserQuestion = ""
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
