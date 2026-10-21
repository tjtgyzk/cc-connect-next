package core

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func bindTestWait(state *interactiveState) *pendingPermission {
	state.currentTurnUserWait = &turnUserWait{}
	pending := state.pending
	pending.waitClock = state.currentTurnUserWait
	pending.waitAgent = state.agentSession
	return pending
}

func TestTurnUserWait_AccumulatesOnlyDeliveredIntervalsAndKeepsSteerClock(t *testing.T) {
	e := newTestEngine()
	s := questionTestState(e, "test:clock", &stubAgentSession{}, testQuestions())
	first := bindTestWait(s)
	start := time.Now()
	s.startPermissionWait(first, start)
	s.startPermissionWait(first, start.Add(time.Second)) // never restart the clock
	s.mu.Lock()
	s.currentMessageID = "steered-message"
	s.presentationGeneration++
	s.finishPermissionWaitLocked(first, start.Add(3*time.Second))
	s.finishPermissionWaitLocked(first, start.Add(9*time.Second)) // duplicate completion
	s.mu.Unlock()
	second := &pendingPermission{waitClock: s.currentTurnUserWait, waitAgent: s.agentSession}
	s.pending = second
	s.startPermissionWait(second, start.Add(10*time.Second))
	s.mu.Lock()
	s.finishPermissionWaitLocked(second, start.Add(12*time.Second))
	s.mu.Unlock()
	if got := s.currentTurnUserWait.duration; got != 5*time.Second {
		t.Fatalf("wait=%v, want two non-overlapping intervals totaling 5s", got)
	}
	undelivered := &pendingPermission{waitClock: s.currentTurnUserWait, waitAgent: s.agentSession}
	s.mu.Lock()
	s.finishPermissionWaitLocked(undelivered, start.Add(100*time.Second))
	s.mu.Unlock()
	if s.currentTurnUserWait.duration != 5*time.Second {
		t.Fatal("failed delivery or transport time was deducted as user wait")
	}
}

func TestTurnUserWait_LateResponseCannotChargeReplacementOrStoppedTurn(t *testing.T) {
	for _, kind := range []string{"queue", "session", "stopped", "recalled"} {
		t.Run(kind, func(t *testing.T) {
			s := questionTestState(newTestEngine(), "test:clock", &stubAgentSession{}, testQuestions())
			pending := bindTestWait(s)
			start := time.Now()
			s.startPermissionWait(pending, start)
			oldClock := s.currentTurnUserWait
			s.mu.Lock()
			switch kind {
			case "queue":
				s.currentTurnUserWait = &turnUserWait{}
			case "session":
				s.agentSession = &recordingAgentSession{}
			case "stopped":
				s.stopped = true
			case "recalled":
				s.discardTurnMessageID = s.currentMessageID
			}
			s.finishPermissionWaitLocked(pending, start.Add(time.Minute))
			s.mu.Unlock()
			if oldClock.duration != 0 || s.currentTurnUserWait.duration != 0 {
				t.Fatal("late callback charged an abandoned or replacement turn")
			}
		})
	}
}

type waitRetrySession struct {
	stubAgentSession
	calls int
}

func (s *waitRetrySession) RespondPermission(string, PermissionResult) error {
	s.calls++
	if s.calls == 1 {
		return errors.New("temporary write failure")
	}
	return nil
}

func TestTurnUserWait_FailedResponsesRemainRetryableWithoutEarlyDeduction(t *testing.T) {
	for _, kind := range []string{"question", "approval", "approve-all", "deny"} {
		t.Run(kind, func(t *testing.T) {
			e, p, agent := newTestEngine(), &stubPlatformEngine{n: "test"}, &waitRetrySession{}
			s := questionTestState(e, "test:retry", agent, testQuestions())
			pending := bindTestWait(s)
			content := "askq:" + questionTestToken + ":0:1"
			if kind != "question" {
				pending.Questions, pending.ToolName = nil, "Bash"
				content = map[string]string{"approval": "allow", "approve-all": "approve all", "deny": "deny"}[kind]
			}
			s.startPermissionWait(pending, time.Now().Add(-2*time.Second))
			for attempt := 0; attempt < 2; attempt++ {
				msg := &Message{SessionKey: "test:retry", UserID: "alice", Content: content, ReplyCtx: "ctx", IsPermissionResponse: kind != "question"}
				if kind == "question" {
					msg = questionTestMessage("test:retry", "alice", content)
				}
				e.handlePendingPermission(p, msg, content, "")
				if kind == "question" && (<-msg.QuestionResponse).Accepted != (attempt == 1) {
					t.Fatal("incorrect question acceptance")
				}
				if attempt == 0 {
					if s.pending != pending || requestDone(pending.Resolved) || pending.responseClaimed || s.currentTurnUserWait.duration != 0 || s.approveAll {
						t.Fatal("failed write prematurely released, deducted or enabled auto-approval")
					}
				}
			}
			if s.pending != nil || !requestDone(pending.Resolved) || s.currentTurnUserWait.duration < 2*time.Second || agent.calls != 2 {
				t.Fatal("successful retry did not complete the original waiting interval")
			}
			if s.approveAll != (kind == "approve-all") {
				t.Fatal("approve-all changed before successful authorization")
			}
		})
	}
}

func TestTurnUserWait_MultiQuestionIsOneNativeWait(t *testing.T) {
	e, p, agent := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}
	s := questionTestState(e, "test:multi-clock", agent, testMultiQuestions())
	pending := bindTestWait(s)
	s.startPermissionWait(pending, time.Now().Add(-time.Second))
	for idx := 0; idx < 2; idx++ {
		msg := questionTestMessage("test:multi-clock", "alice", fmt.Sprintf("askq:%s:%d:1", questionTestToken, idx))
		e.handlePendingPermission(p, msg, msg.Content, "")
		if !(<-msg.QuestionResponse).Accepted {
			t.Fatal("valid answer rejected")
		}
		if idx == 0 && (s.currentTurnUserWait.duration != 0 || agent.calls != 0 || s.pending != pending) {
			t.Fatal("partial answer ended the native request or duplicated its wait")
		}
	}
	if agent.calls != 1 || s.currentTurnUserWait.duration < time.Second {
		t.Fatal("whole request was not resumed and timed exactly once")
	}
}

type blockedQuestionDelivery struct {
	*turnRecordPlatform
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockedQuestionDelivery) Send(ctx context.Context, replyCtx any, content string) error {
	if strings.HasPrefix(content, "❓") {
		p.once.Do(func() { close(p.entered) })
		<-p.release
	}
	return p.turnRecordPlatform.Send(ctx, replyCtx, content)
}

func TestTurnUserWait_TransportBeforeActionablePromptIsExcluded(t *testing.T) {
	p := &blockedQuestionDelivery{turnRecordPlatform: newTurnRecordPlatform(), entered: make(chan struct{}), release: make(chan struct{})}
	sess := newQueuingSession("delivery")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	var released sync.Once
	t.Cleanup(func() { released.Do(func() { close(p.release) }); _ = e.Stop() })
	key := "test:delivery"
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "original", UserID: "alice", Content: "check", ReplyCtx: "ctx"})
	waitForSteer(t, "agent started", func() bool { return turnRecordingSendCount(sess) == 1 })
	sess.events <- Event{Type: EventPermissionRequest, RequestID: "req", ToolName: "AskUserQuestion", Questions: testQuestions()}
	select {
	case <-p.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("prompt delivery not started")
	}
	e.interactiveMu.Lock()
	state := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	state.mu.Lock()
	started := state.pending.waitStartedAt
	state.mu.Unlock()
	if !started.IsZero() {
		t.Fatal("outgoing send latency counted as user wait")
	}
	released.Do(func() { close(p.release) })
	waitForSteer(t, "delivered wait starts", func() bool { state.mu.Lock(); defer state.mu.Unlock(); return !state.pending.waitStartedAt.IsZero() })
}

func TestTurnUserWait_ApprovalNativeCompletionCannotOvertakeSuccessfulWrite(t *testing.T) {
	p := newTurnRecordPlatform()
	sess := &questionCompletionRaceSession{
		queuingAgentSession: newQueuingSession("approval-race"), permissionDone: make(chan struct{}),
		responseEntered: make(chan struct{}), responseRelease: make(chan struct{}),
	}
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(sess.responseRelease) }); _ = e.Stop() })
	key := "test:approval-race"
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "original", UserID: "alice", Content: "check", ReplyCtx: "ctx"})
	waitForSteer(t, "native send", func() bool { return turnRecordingSendCount(sess.queuingAgentSession) == 1 })
	sess.events <- Event{Type: EventPermissionRequest, RequestID: "approval", ToolName: "Bash", PermissionDone: sess.permissionDone}
	var state *interactiveState
	waitForSteer(t, "approval actionable", func() bool {
		e.interactiveMu.Lock()
		state = e.interactiveStates[key]
		e.interactiveMu.Unlock()
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.pending != nil && !state.pending.waitStartedAt.IsZero()
	})
	answered := make(chan struct{})
	go func() {
		e.ReceiveMessage(p, &Message{SessionKey: key, UserID: "alice", Content: "allow", ReplyCtx: "ctx", IsPermissionResponse: true})
		close(answered)
	}()
	select {
	case <-sess.responseEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("approval write not reached")
	}
	// A duplicate must not call the native session a second time (the fixture's
	// close(responseEntered) would panic), nor release its first blocked call.
	e.ReceiveMessage(p, &Message{SessionKey: key, UserID: "alice", Content: "allow", ReplyCtx: "ctx", IsPermissionResponse: true})
	state.mu.Lock()
	got := state.currentTurnUserWait.duration
	state.mu.Unlock()
	if got != 0 || len(p.snapshot()) != 0 {
		t.Fatal("native done signal prematurely finalized an unacknowledged approval")
	}
	release.Do(func() { close(sess.responseRelease) })
	select {
	case <-answered:
	case <-time.After(2 * time.Second):
		t.Fatal("successful write did not release callback")
	}
	waitForSteer(t, "completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
	rows := p.snapshot()
	if len(rows) != 1 || rows[0].UserWaitTiming != "measured" || rows[0].UserWaitDuration <= 0 || rows[0].UserWaitDuration > rows[0].Duration {
		t.Fatal("manual approval did not archive one bounded measured wait")
	}
}

func TestTurnUserWait_AutomaticApprovalDoesNotDeductTime(t *testing.T) {
	p, sess := newTurnRecordPlatform(), newQueuingSession("auto-approve")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	key := "test:auto-approve"
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", MessageID: "original", UserID: "alice", Content: "check", ReplyCtx: "ctx"})
	waitForSteer(t, "native send", func() bool { return turnRecordingSendCount(sess) == 1 })
	e.interactiveMu.Lock()
	state := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	state.mu.Lock()
	state.approveAll = true
	state.mu.Unlock()
	sess.events <- Event{Type: EventPermissionRequest, RequestID: "automatic", ToolName: "Bash"}
	sess.events <- Event{Type: EventResult, Content: "done", Done: true}
	waitForSteer(t, "auto approved completion", func() bool { return !e.sessions.GetOrCreateActive(key).Busy() })
	rows := p.snapshot()
	if len(rows) != 1 || rows[0].UserWaitTiming != "measured" || rows[0].UserWaitDuration != 0 {
		t.Fatal("automatic approval was mistaken for a user wait")
	}
}

func TestUserWaitPromptDeliveryFailuresAreObservable(t *testing.T) {
	e, p := newTestEngine(), newTurnRecordPlatform()
	p.sendError = errors.New("delivery unavailable")
	if e.sendAskQuestionPrompt(p, "ctx", testQuestions(), 0, questionTestToken) == nil {
		t.Fatal("failed question prompt reported actionable")
	}
	if e.sendPermissionPrompt(p, "ctx", "permission", "Bash", "") == nil {
		t.Fatal("failed permission prompt reported actionable")
	}
}

type timedWaitResponseSession struct {
	stubAgentSession
	failFirst bool
	entered   []time.Time
	returned  []time.Time
}

func (s *timedWaitResponseSession) RespondPermission(string, PermissionResult) error {
	s.entered = append(s.entered, time.Now())
	time.Sleep(60 * time.Millisecond)
	s.returned = append(s.returned, time.Now())
	if s.failFirst && len(s.entered) == 1 {
		return errors.New("retry the native write")
	}
	return nil
}

func TestTurnUserWait_ExcludesResponseRPCIncludingFailedAttempts(t *testing.T) {
	for _, kind := range []string{"question", "approval"} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retry=%t", kind, retry), func(t *testing.T) {
				e, p := newTestEngine(), &stubPlatformEngine{n: "test"}
				agent := &timedWaitResponseSession{failFirst: retry}
				state := questionTestState(e, "test:rpc-timing", agent, testQuestions())
				pending := bindTestWait(state)
				start := time.Now().Add(-time.Second)
				state.startPermissionWait(pending, start)
				content := "askq:" + questionTestToken + ":0:1"
				if kind == "approval" {
					content, pending.ToolName, pending.Questions = "allow", "Bash", nil
				}
				attempts := 1
				if retry {
					attempts++
				}
				for i := 0; i < attempts; i++ {
					msg := &Message{SessionKey: "test:rpc-timing", UserID: "alice", ReplyCtx: "ctx", Content: content, IsPermissionResponse: kind == "approval"}
					if kind == "question" {
						msg = questionTestMessage("test:rpc-timing", "alice", content)
					}
					e.handlePendingPermission(p, msg, content, "")
					if retry && i == 0 {
						if state.currentTurnUserWait.duration != 0 || pending.waitStartedAt.Before(agent.returned[0]) {
							t.Fatal("failed RPC committed waiting or restarted before the write returned")
						}
					}
				}
				upperBound := agent.entered[0].Sub(start)
				if retry {
					upperBound += agent.entered[1].Sub(agent.returned[0])
				}
				if got := state.currentTurnUserWait.duration; got < time.Second || got > upperBound {
					t.Fatalf("wait %v must only include user input segments, bounded by %v", got, upperBound)
				}
			})
		}
	}
}

type slowNextQuestionPlatform struct {
	stubPlatformEngine
	entered, returned time.Time
}

func (p *slowNextQuestionPlatform) Send(ctx context.Context, replyCtx any, content string) error {
	p.entered = time.Now()
	time.Sleep(60 * time.Millisecond)
	p.returned = time.Now()
	return p.stubPlatformEngine.Send(ctx, replyCtx, content)
}

func TestTurnUserWait_ExcludesDeliveryBetweenMultipleQuestions(t *testing.T) {
	e, p, agent := newTestEngine(), &slowNextQuestionPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}, &recordingAgentSession{}
	state := questionTestState(e, "test:multi-delivery", agent, testMultiQuestions())
	pending := bindTestWait(state)
	start := time.Now().Add(-time.Second)
	state.startPermissionWait(pending, start)
	first := questionTestMessage("test:multi-delivery", "alice", "askq:"+questionTestToken+":0:1")
	e.handlePendingPermission(p, first, first.Content, "")
	if !(<-first.QuestionResponse).Accepted || agent.calls != 0 || pending.waitStartedAt.Before(p.returned) {
		t.Fatal("second wait began before the next question became actionable")
	}
	firstUpperBound := p.entered.Sub(start)
	if pending.waitDuration > firstUpperBound || state.currentTurnUserWait.duration != 0 {
		t.Fatal("card switching latency was counted or partial response committed")
	}
	second := questionTestMessage("test:multi-delivery", "alice", "askq:"+questionTestToken+":1:1")
	e.handlePendingPermission(p, second, second.Content, "")
	secondUpperBound := time.Since(p.returned)
	if got := state.currentTurnUserWait.duration; got < time.Second || got > firstUpperBound+secondUpperBound || agent.calls != 1 {
		t.Fatalf("multi-question user wait includes card switching: wait=%v upper=%v", got, firstUpperBound+secondUpperBound)
	}
}

func TestTurnUserWait_ExpiredBindingNeverSendsOldRequestToReplacementAgent(t *testing.T) {
	for _, question := range []bool{false, true} {
		e, p, original, replacement := newTestEngine(), &stubPlatformEngine{n: "test"}, &recordingAgentSession{}, &recordingAgentSession{}
		state := questionTestState(e, "test:replaced", original, testQuestions())
		pending := bindTestWait(state)
		state.startPermissionWait(pending, time.Now().Add(-time.Second))
		state.agentSession = replacement
		msg := questionTestMessage("test:replaced", "alice", "askq:"+questionTestToken+":0:1")
		if !question {
			pending.ToolName, pending.Questions = "Bash", nil
			msg.IsQuestionResponse, msg.IsPermissionResponse, msg.Content = false, true, "allow"
		}
		e.handlePendingPermission(p, msg, msg.Content, "")
		if original.calls != 0 || replacement.calls != 0 || state.currentTurnUserWait.duration != 0 {
			t.Fatal("stale request reached a replacement agent or polluted its clock")
		}
	}
}
