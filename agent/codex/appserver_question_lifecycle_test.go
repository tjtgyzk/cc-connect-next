package codex

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/timmyagentic/cc-connect-next/core"
)

func pendingQuestionSession(t *testing.T) (*appServerSession, core.Event, *lockedWriteCloser) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &lockedWriteCloser{}
	s := &appServerSession{ctx: ctx, cancel: cancel, stdin: w, events: make(chan core.Event, 8), currentTurn: "turn-1"}
	s.threadID.Store("thread-1")
	s.handleServerRequest(serverRequestProbe(t, `"question-1"`, "item/tool/requestUserInput", map[string]any{
		"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1",
		"questions": []any{map[string]any{"id": "environment", "header": "Environment", "question": "prod or stg?",
			"options": []any{map[string]any{"label": "prod"}, map[string]any{"label": "stg"}}}},
	}))
	select {
	case event := <-s.events:
		return s, event, w
	case <-time.After(time.Second):
		t.Fatal("question event was not emitted")
		return nil, core.Event{}, nil
	}
}

func TestAppServerQuestion_ConcurrentAnswersConsumeRequestExactlyOnce(t *testing.T) {
	s, event, w := pendingQuestionSession(t)
	if event.PermissionDone == nil || event.Questions[0].ID != "environment" {
		t.Fatal("question must preserve its native identity and lifecycle")
	}
	var wg sync.WaitGroup
	results := make(chan error, 20)
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- s.RespondPermission(event.RequestID, core.PermissionResult{Behavior: "allow", UpdatedInput: map[string]any{
				"answers": map[string]any{"environment": "prod"},
			}})
		}()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d answers, want exactly one", accepted)
	}
	select {
	case <-event.PermissionDone:
	case <-time.After(time.Second):
		t.Fatal("response did not finish lifecycle")
	}
	var response struct {
		ID     string                            `json:"id"`
		Result appServerRequestUserInputResponse `json:"result"`
	}
	if err := json.Unmarshal([]byte(w.String()), &response); err != nil {
		t.Fatal(err)
	}
	if response.ID != "question-1" || response.Result.Answers["environment"].Answers[0] != "prod" {
		t.Fatalf("wrong native response: %#v", response)
	}
}

func TestAppServerQuestion_NativeCancellationInvalidatesWithoutSendingEmptyAnswer(t *testing.T) {
	for _, action := range []string{"resolved", "turn_completed", "turn_failed", "connection_closed", "context_canceled"} {
		t.Run(action, func(t *testing.T) {
			s, event, w := pendingQuestionSession(t)
			switch action {
			case "resolved":
				s.handleNotification("serverRequest/resolved", json.RawMessage(`{"threadId":"thread-1","requestId":"question-1"}`))
			case "turn_completed":
				s.completeTurn()
			case "turn_failed":
				s.failTurn(context.Canceled)
			case "connection_closed":
				s.rejectPendingApprovals(context.Canceled)
			case "context_canceled":
				s.cancel()
			}
			select {
			case <-event.PermissionDone:
			case <-time.After(time.Second):
				t.Fatal("pending lifecycle leaked after cancellation")
			}
			if err := s.RespondPermission(event.RequestID, core.PermissionResult{Behavior: "allow"}); err == nil {
				t.Fatal("stale answer accepted")
			}
			if got := w.String(); got != "" {
				t.Fatalf("cancellation must not resume model with empty answers: %s", got)
			}
		})
	}
}

func TestAppServerQuestion_OtherThreadResolutionDoesNotCancel(t *testing.T) {
	s, event, _ := pendingQuestionSession(t)
	s.handleNotification("serverRequest/resolved", json.RawMessage(`{"threadId":"different-thread","requestId":"question-1"}`))
	select {
	case <-event.PermissionDone:
		t.Fatal("other thread canceled this question")
	default:
	}
	if err := s.RespondPermission(event.RequestID, core.PermissionResult{Behavior: "deny"}); err != nil {
		t.Fatal(err)
	}
}

func TestAppServerQuestion_IDsDisambiguateIdenticalQuestionText(t *testing.T) {
	questions := []appServerRequestUserInputQuestion{{ID: "first", Question: "Environment?"}, {ID: "second", Question: "Environment?"}}
	response := appServerRequestUserInputResponseFromResult(questions, core.PermissionResult{Behavior: "allow", UpdatedInput: map[string]any{
		"answers": map[string]any{"first": "prod", "second": "stg"},
	}})
	if response.Answers["first"].Answers[0] != "prod" || response.Answers["second"].Answers[0] != "stg" {
		t.Fatalf("IDs were not preserved: %#v", response)
	}
}

type gatedPermissionWriter struct{ entered, release chan struct{} }

func (w *gatedPermissionWriter) Write([]byte) (int, error) {
	close(w.entered)
	<-w.release
	return 0, errors.New("isolated broken pipe")
}
func (*gatedPermissionWriter) Close() error { return nil }

func TestAppServerQuestion_ResponseWaitsForNativeWriteAndReportsFailure(t *testing.T) {
	s, event, _ := pendingQuestionSession(t)
	w := &gatedPermissionWriter{entered: make(chan struct{}), release: make(chan struct{})}
	s.procMu.Lock()
	s.stdin = w
	s.procMu.Unlock()
	result := make(chan error, 1)
	go func() {
		result <- s.RespondPermission(event.RequestID, core.PermissionResult{Behavior: "allow", UpdatedInput: map[string]any{"answers": map[string]any{"environment": "stg"}}})
	}()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("native response was not written")
	}
	select {
	case err := <-result:
		t.Fatalf("reported answer before writing it: %v", err)
	default:
	}
	close(w.release)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("failed write reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("failed write did not settle answer")
	}
	select {
	case <-event.PermissionDone:
	case <-time.After(time.Second):
		t.Fatal("failed write leaked pending question")
	}
	select {
	case failure := <-s.Events():
		if failure.Type != core.EventError {
			t.Fatalf("failed response did not terminate turn: %#v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("failed response left the engine waiting")
	}
	if s.ctx.Err() == nil || s.Alive() {
		t.Fatal("failed response did not close its isolated session")
	}
}

func TestAppServerApproval_TimeoutClaimsRequestBeforeLateAnswer(t *testing.T) {
	s := &appServerSession{ctx: context.Background()}
	pending := s.registerPendingApproval("expired")
	result, respond := s.waitForApproval(pending, time.Nanosecond)
	if !respond || result.Behavior != "deny" {
		t.Fatalf("timeout did not deny: %#v", result)
	}
	if err := s.RespondPermission("expired", core.PermissionResult{Behavior: "allow"}); err == nil {
		t.Fatal("accepted answer after timeout decision")
	}
	s.finishPendingApproval("expired", pending)
}

func TestAppServerQuestion_WrittenAnswerWinsSubsequentCancellation(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		s := &appServerSession{ctx: ctx}
		pending := s.registerPendingApproval("written")
		pending.acknowledge(nil) // successful native write is already known
		cancel()
		if err := s.waitForApprovalWrite("written", pending); err != nil {
			t.Fatalf("successful native write was changed to cancellation: %v", err)
		}
		s.finishPendingApproval("written", pending)
	}
}
