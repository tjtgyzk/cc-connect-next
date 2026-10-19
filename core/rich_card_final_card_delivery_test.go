package core

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type stubRichCardFinalDeliveryPlatform struct {
	*stubRichCardTerminalTextPlatform
	requireNew bool
	failFinal  bool
	policy     func(string) bool
	transform  func(string) string

	deliveryMu     sync.Mutex
	policyMarkdown []string
	policyReplyCtx []any
	operations     []string
	finalSent      chan struct{}
	releaseFinal   chan struct{}
}

func (p *stubRichCardFinalDeliveryPlatform) RequiresNewFinalCard(_ context.Context, replyCtx any, markdown string) bool {
	p.deliveryMu.Lock()
	defer p.deliveryMu.Unlock()
	p.policyMarkdown = append(p.policyMarkdown, markdown)
	p.policyReplyCtx = append(p.policyReplyCtx, replyCtx)
	if p.policy != nil {
		return p.policy(markdown)
	}
	return p.requireNew
}

func (p *stubRichCardFinalDeliveryPlatform) TransformRichCardMarkdown(_ context.Context, _ any, markdown string) string {
	if p.transform != nil {
		return p.transform(markdown)
	}
	return markdown
}

func (p *stubRichCardFinalDeliveryPlatform) SendPreviewStart(ctx context.Context, replyCtx any, content string) (any, error) {
	final := strings.Contains(content, "status=done")
	if final && p.failFinal {
		return nil, errors.New("final card send failed")
	}
	handle, err := p.stubRichCardSilentPlatform.SendPreviewStart(ctx, replyCtx, content)
	if err != nil {
		return handle, err
	}
	p.deliveryMu.Lock()
	p.operations = append(p.operations, fmt.Sprint("sent:", handle))
	p.deliveryMu.Unlock()
	if final && p.finalSent != nil {
		close(p.finalSent)
		// Model an API request accepted before recall, with its successful
		// handle arriving after the delivery context has been canceled.
		<-p.releaseFinal
	}
	return handle, nil
}

func (p *stubRichCardFinalDeliveryPlatform) DeletePreviewMessage(ctx context.Context, handle any) error {
	p.deliveryMu.Lock()
	p.operations = append(p.operations, fmt.Sprint("deleted:", handle))
	p.deliveryMu.Unlock()
	return p.stubRichCardTerminalTextPlatform.DeletePreviewMessage(ctx, handle)
}

func (p *stubRichCardFinalDeliveryPlatform) deliverySnapshot() (markdown []string, replyContexts []any, operations []string) {
	p.deliveryMu.Lock()
	defer p.deliveryMu.Unlock()
	return append([]string(nil), p.policyMarkdown...), append([]any(nil), p.policyReplyCtx...), append([]string(nil), p.operations...)
}

func newRichCardFinalDeliveryTest(t *testing.T, requireNew bool) (*Engine, *stubRichCardFinalDeliveryPlatform, *Session, *interactiveState, string) {
	t.Helper()
	base := &stubRichCardSilentPlatform{stubPlatformEngine: stubPlatformEngine{n: "rich-final"}}
	p := &stubRichCardFinalDeliveryPlatform{
		stubRichCardTerminalTextPlatform: &stubRichCardTerminalTextPlatform{stubRichCardSilentPlatform: base},
		requireNew:                       requireNew,
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	e.SetDisplayConfig(DisplayCfg{Mode: "compact", CardMode: "rich"})
	e.SetStreamPreviewCfg(StreamPreviewCfg{Enabled: false})
	e.SetReplyFooterEnabled(true)
	sessionKey := "rich-final:" + t.Name()
	session := e.sessions.GetOrCreateActive(sessionKey)
	state := &interactiveState{
		agentSession: newControllableSession("session-final-card"),
		platform:     p,
		replyCtx:     &struct{ messageID string }{"trigger-message"},
	}
	e.interactiveStates[sessionKey] = state
	return e, p, session, state, sessionKey
}

func finishRichCardFinalDeliveryTest(e *Engine, session *Session, state *interactiveState, sessionKey, answer string) {
	state.agentSession.(*controllableAgentSession).events <- Event{Type: EventResult, Content: answer, Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "trigger-message", time.Now(), nil, nil, state.replyCtx)
}

func TestRichCardFinalDeliveryPolicySendsRichReplacementBeforeDeletingPreview(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalDeliveryTest(t, true)
	answer := "请 @Reviewer-Bot 复核\n\n| Result | Value |\n| --- | --- |\n| tests | passed |\n\n![report](https://example.com/report.png)\n\n```go\nfmt.Println(\"done\")\n```"
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, answer)

	starts, _, updates, _ := p.stubRichCardSilentPlatform.snapshot()
	if len(starts) != 2 || !strings.Contains(starts[1], "status=done") || !strings.Contains(starts[1], fmt.Sprintf("body=%q", answer)) || !strings.Contains(starts[1], "footer=⏱ ") {
		t.Fatalf("final rich card did not preserve markdown and footer: %v", starts)
	}
	for _, update := range updates {
		if strings.Contains(update, "status=done") {
			t.Fatalf("final answer patched the old card: %v", updates)
		}
	}
	contexts := p.previewStartContexts()
	if len(contexts) != 2 || contexts[0] != state.replyCtx || contexts[1] != state.replyCtx {
		t.Fatalf("card sends lost the triggering reply context: %v", contexts)
	}
	markdown, policyContexts, operations := p.deliverySnapshot()
	if !reflect.DeepEqual(markdown, []string{answer, answer}) || len(policyContexts) != 2 || policyContexts[0] != state.replyCtx || policyContexts[1] != state.replyCtx {
		t.Fatalf("policy input = %v, contexts = %v", markdown, policyContexts)
	}
	if want := []string{"sent:handle-1", "sent:handle-2", "deleted:handle-1"}; !reflect.DeepEqual(operations, want) {
		t.Fatalf("delivery order = %v, want %v", operations, want)
	}
	prepared, texts, deleted := p.terminalSnapshot()
	if len(prepared) != 0 || len(texts) != 0 || !reflect.DeepEqual(deleted, []any{"handle-1"}) {
		t.Fatalf("new-card policy did not bypass terminal text: prepared=%v, texts=%v, deleted=%v", prepared, texts, deleted)
	}
	history := session.GetHistory(0)
	if len(history) == 0 || history[len(history)-1].Content != answer {
		t.Fatalf("assistant history = %+v", history)
	}
}

func TestRichCardFinalDeliveryPolicyRevalidatesMentionsAfterMarkdownTransform(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalDeliveryTest(t, true)
	p.policy = func(markdown string) bool {
		return strings.HasPrefix(markdown, `<at user_id="ou_requester">`) && strings.Count(markdown, "<at ") == 1
	}
	p.transform = func(markdown string) string {
		return strings.ReplaceAll(markdown, "@Other", `<at user_id="ou_other">Other</at>`)
	}
	answer := `<at user_id="ou_requester">Requester</at> 完成，请 @Other 复核`
	finalBody := p.transform(answer)
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, answer)

	markdown, _, _ := p.deliverySnapshot()
	if !reflect.DeepEqual(markdown, []string{answer, finalBody}) {
		t.Fatalf("policy did not validate the prepared body: %v", markdown)
	}
	starts, _, updates, _ := p.stubRichCardSilentPlatform.snapshot()
	prepared, texts, deleted := p.terminalSnapshot()
	if len(starts) != 1 || len(prepared) != 0 || len(texts) != 0 || len(deleted) != 0 {
		t.Fatalf("transformed extra mention caused a new send: starts=%v, prepared=%v, texts=%v, deleted=%v", starts, prepared, texts, deleted)
	}
	if len(updates) == 0 || !strings.Contains(updates[len(updates)-1], "status=done") || !strings.Contains(updates[len(updates)-1], fmt.Sprintf("body=%q", finalBody)) {
		t.Fatalf("prepared answer did not update the existing card: %v", updates)
	}
}

func TestRichCardFinalDeliveryPolicySendFailurePreservesPreview(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalDeliveryTest(t, true)
	p.failFinal = true
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, "final answer")

	starts, _, updates, _ := p.stubRichCardSilentPlatform.snapshot()
	_, _, deleted := p.terminalSnapshot()
	if len(starts) != 1 || len(deleted) != 0 {
		t.Fatalf("failed replacement removed the original card: starts=%v, deleted=%v", starts, deleted)
	}
	if len(updates) == 0 || !strings.Contains(updates[len(updates)-1], "status=error") {
		t.Fatalf("failed replacement did not mark the original card failed: %v", updates)
	}
	for _, entry := range session.GetHistory(0) {
		if entry.Role == "assistant" {
			t.Fatalf("undelivered final answer entered history: %+v", entry)
		}
	}
}

func TestRichCardFinalDeliveryPolicyRecallDeletesLateReplacement(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalDeliveryTest(t, true)
	p.finalSent = make(chan struct{})
	p.releaseFinal = make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(p.releaseFinal) }) }
	defer release()
	turnDone := make(chan struct{})
	go func() {
		finishRichCardFinalDeliveryTest(e, session, state, sessionKey, "final answer")
		close(turnDone)
	}()
	select {
	case <-p.finalSent:
	case <-time.After(time.Second):
		t.Fatal("replacement card was not sent")
	}
	state.cancelTurnSilently("trigger-message")
	release()
	select {
	case <-turnDone:
	case <-time.After(time.Second):
		t.Fatal("recalled final-card turn did not finish")
	}
	_, _, deleted := p.terminalSnapshot()
	if !reflect.DeepEqual(deleted, []any{"handle-1", "handle-2"}) {
		t.Fatalf("recall left a card visible: deleted=%v", deleted)
	}
	for _, entry := range session.GetHistory(0) {
		if entry.Role == "assistant" {
			t.Fatalf("recalled final answer entered history: %+v", entry)
		}
	}
}

func TestRichCardFinalDeliveryPolicyOrdinaryAnswerKeepsLifecycleCard(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalDeliveryTest(t, false)
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, "ordinary answer")

	starts, _, updates, _ := p.stubRichCardSilentPlatform.snapshot()
	_, _, deleted := p.terminalSnapshot()
	if len(starts) != 1 || len(deleted) != 0 || len(updates) == 0 || !strings.Contains(updates[len(updates)-1], "status=done") {
		t.Fatalf("ordinary answer did not retain the lifecycle card: starts=%v, updates=%v, deleted=%v", starts, updates, deleted)
	}
	markdown, _, _ := p.deliverySnapshot()
	if !reflect.DeepEqual(markdown, []string{"ordinary answer"}) {
		t.Fatalf("a platform without a final preparer rechecked a false policy: %v", markdown)
	}
}

func TestRichCardFinalDeliveryPolicyFalsePreservesTerminalTextFallback(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalDeliveryTest(t, false)
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, "请 @Reviewer-Bot 复核")

	starts, _, _, _ := p.stubRichCardSilentPlatform.snapshot()
	prepared, texts, deleted := p.terminalSnapshot()
	if len(starts) != 1 || len(prepared) != 1 || len(texts) != 1 || !strings.Contains(texts[0], `user_id="ou_bot"`) || !reflect.DeepEqual(deleted, []any{"handle-1"}) {
		t.Fatalf("terminal text behavior changed: starts=%v, prepared=%v, texts=%v, deleted=%v", starts, prepared, texts, deleted)
	}
}

func TestRichCardFinalDeliveryPolicyNoReplyDoesNotSendFinalCard(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalDeliveryTest(t, true)
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, "NO_REPLY")

	starts, _, updates, _ := p.stubRichCardSilentPlatform.snapshot()
	markdown, _, _ := p.deliverySnapshot()
	prepared, texts, deleted := p.terminalSnapshot()
	if len(starts) != 1 || len(markdown) != 0 || len(prepared) != 0 || len(texts) != 0 || !reflect.DeepEqual(deleted, []any{"handle-1"}) {
		t.Fatalf("NO_REPLY produced a final delivery: starts=%v, policy=%v, prepared=%v, texts=%v, deleted=%v", starts, markdown, prepared, texts, deleted)
	}
	for _, update := range updates {
		if strings.Contains(update, "status=done") {
			t.Fatalf("NO_REPLY finalized the preview: %v", updates)
		}
	}
}

const finalRequesterMention = `<at user_id="ou_requester">Requester</at>` + "\n\n"

type stubRichCardFinalPreparerPlatform struct {
	*stubRichCardFinalDeliveryPlatform
	prepareMu       sync.Mutex
	finalMarkdown   []string
	finalReplyCtx   []any
	terminalSendErr error
}

func (p *stubRichCardFinalPreparerPlatform) PrepareRichCardFinalMarkdown(_ context.Context, replyCtx any, markdown string) string {
	p.prepareMu.Lock()
	defer p.prepareMu.Unlock()
	p.finalMarkdown = append(p.finalMarkdown, markdown)
	p.finalReplyCtx = append(p.finalReplyCtx, replyCtx)
	return finalRequesterMention + markdown
}

func (p *stubRichCardFinalPreparerPlatform) SendRichCardTerminalText(ctx context.Context, replyCtx any, content string) (any, error) {
	if p.terminalSendErr != nil {
		return nil, p.terminalSendErr
	}
	return p.stubRichCardTerminalTextPlatform.SendRichCardTerminalText(ctx, replyCtx, content)
}

func (p *stubRichCardFinalPreparerPlatform) preparationSnapshot() ([]string, []any) {
	p.prepareMu.Lock()
	defer p.prepareMu.Unlock()
	return append([]string(nil), p.finalMarkdown...), append([]any(nil), p.finalReplyCtx...)
}

func newRichCardFinalPreparerTest(t *testing.T) (*Engine, *stubRichCardFinalPreparerPlatform, *Session, *interactiveState, string) {
	t.Helper()
	e, base, session, state, sessionKey := newRichCardFinalDeliveryTest(t, false)
	base.policy = func(markdown string) bool { return strings.HasPrefix(markdown, finalRequesterMention) }
	p := &stubRichCardFinalPreparerPlatform{stubRichCardFinalDeliveryPlatform: base}
	state.platform = p
	return e, p, session, state, sessionKey
}

func TestRichCardFinalPreparerAddsRequesterOnlyToDeliveredFinalCard(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalPreparerTest(t)
	e.SetStreamPreviewCfg(StreamPreviewCfg{Enabled: true, IntervalMs: 1, MinDeltaChars: 1})
	p.transform = func(markdown string) string { return strings.ReplaceAll(markdown, "raw-link", "resolved-link") }
	answer := "ordinary answer with raw-link"
	resolved := p.transform(answer)
	finalBody := finalRequesterMention + resolved
	state.agentSession.(*controllableAgentSession).events <- Event{Type: EventText, Content: answer}
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, answer)

	prepared, contexts := p.preparationSnapshot()
	if !reflect.DeepEqual(prepared, []string{resolved}) || len(contexts) != 1 || contexts[0] != state.replyCtx {
		t.Fatalf("final preparation lost resolved body or reply context: prepared=%v, contexts=%v", prepared, contexts)
	}
	markdown, _, operations := p.deliverySnapshot()
	if !reflect.DeepEqual(markdown, []string{answer, finalBody}) {
		t.Fatalf("policy did not recheck an initially unmentioned answer: %v", markdown)
	}
	starts, streams, updates, _ := p.stubRichCardSilentPlatform.snapshot()
	if len(starts) != 2 || !strings.Contains(starts[1], "status=done") || !strings.Contains(starts[1], fmt.Sprintf("body=%q", finalBody)) {
		t.Fatalf("prepared answer was not delivered once as a completed card: %v", starts)
	}
	if len(streams) == 0 {
		t.Fatal("test did not exercise preview streaming")
	}
	for _, preview := range append(append([]string{starts[0]}, streams...), updates...) {
		if strings.Contains(preview, "ou_requester") || strings.Contains(preview, "status=done") {
			t.Fatalf("final preparation leaked into the lifecycle preview: %q", preview)
		}
	}
	if want := []string{"sent:handle-1", "sent:handle-2", "deleted:handle-1"}; !reflect.DeepEqual(operations, want) {
		t.Fatalf("delivery order = %v, want %v", operations, want)
	}
	terminalPrepared, texts, _ := p.terminalSnapshot()
	if !reflect.DeepEqual(terminalPrepared, []string{answer}) || len(texts) != 0 {
		t.Fatalf("final preparation changed initial terminal fallback input: prepared=%v, texts=%v", terminalPrepared, texts)
	}
	history := session.GetHistory(0)
	if len(history) == 0 || history[len(history)-1].Content != answer {
		t.Fatalf("final-only presentation changed assistant history: %+v", history)
	}
}

func TestRichCardFinalPreparerStillHonorsFinalPolicy(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalPreparerTest(t)
	p.policy = func(string) bool { return false }
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, "ordinary answer")

	markdown, _, _ := p.deliverySnapshot()
	if !reflect.DeepEqual(markdown, []string{"ordinary answer", finalRequesterMention + "ordinary answer"}) {
		t.Fatalf("policy inputs = %v", markdown)
	}
	starts, _, updates, _ := p.stubRichCardSilentPlatform.snapshot()
	_, _, deleted := p.terminalSnapshot()
	if len(starts) != 1 || len(deleted) != 0 || len(updates) == 0 || !strings.Contains(updates[len(updates)-1], "status=done") {
		t.Fatalf("false final policy did not retain the lifecycle card: starts=%v, updates=%v, deleted=%v", starts, updates, deleted)
	}
}

func TestRichCardFinalPreparerSkipsSilentTextFallbackAndError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event Event
	}{
		{"silent", Event{Type: EventResult, Content: "NO_REPLY", Done: true}},
		{"text_fallback", Event{Type: EventResult, Content: "请 @Reviewer-Bot 复核", Done: true}},
		{"error", Event{Type: EventError, Error: errors.New("agent failed")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, p, session, state, sessionKey := newRichCardFinalPreparerTest(t)
			state.agentSession.(*controllableAgentSession).events <- tc.event
			e.processInteractiveEvents(state, session, e.sessions, sessionKey, "trigger-message", time.Now(), nil, nil, state.replyCtx)
			prepared, _ := p.preparationSnapshot()
			if len(prepared) != 0 {
				t.Fatalf("non-card terminal branch invoked final preparation: %v", prepared)
			}
			if tc.name == "text_fallback" {
				_, texts, deleted := p.terminalSnapshot()
				if len(texts) != 1 || !strings.Contains(texts[0], `user_id="ou_bot"`) || !reflect.DeepEqual(deleted, []any{"handle-1"}) {
					t.Fatalf("bot handoff text delivery changed: texts=%v, deleted=%v", texts, deleted)
				}
			}
		})
	}
}

func TestRichCardFinalPreparerRunsAfterTerminalTextSendFailure(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalPreparerTest(t)
	p.terminalSendErr = errors.New("terminal text send failed")
	answer := "请 @Reviewer-Bot 复核"
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, answer)
	prepared, _ := p.preparationSnapshot()
	starts, _, _, _ := p.stubRichCardSilentPlatform.snapshot()
	if !reflect.DeepEqual(prepared, []string{answer}) || len(starts) != 2 || !strings.Contains(starts[1], "status=done") {
		t.Fatalf("failed text fallback prevented final card preparation: prepared=%v, starts=%v", prepared, starts)
	}
}

func TestRichCardFinalPreparerSendFailurePreservesPreviewAndHistory(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalPreparerTest(t)
	p.failFinal = true
	finishRichCardFinalDeliveryTest(e, session, state, sessionKey, "ordinary answer")
	prepared, _ := p.preparationSnapshot()
	starts, _, updates, _ := p.stubRichCardSilentPlatform.snapshot()
	_, _, deleted := p.terminalSnapshot()
	if len(prepared) != 1 || len(starts) != 1 || len(deleted) != 0 || len(updates) == 0 || !strings.Contains(updates[len(updates)-1], "status=error") {
		t.Fatalf("failed prepared delivery changed preview handling: prepared=%v, starts=%v, updates=%v, deleted=%v", prepared, starts, updates, deleted)
	}
	for _, entry := range session.GetHistory(0) {
		if entry.Role == "assistant" {
			t.Fatalf("undelivered prepared answer entered history: %+v", entry)
		}
	}
}

func TestRichCardFinalPreparerRecallDeletesLateReplacement(t *testing.T) {
	e, p, session, state, sessionKey := newRichCardFinalPreparerTest(t)
	p.finalSent = make(chan struct{})
	p.releaseFinal = make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(p.releaseFinal) }) }
	defer release()
	turnDone := make(chan struct{})
	go func() {
		finishRichCardFinalDeliveryTest(e, session, state, sessionKey, "ordinary answer")
		close(turnDone)
	}()
	select {
	case <-p.finalSent:
	case <-time.After(time.Second):
		t.Fatal("prepared final card was not sent")
	}
	state.cancelTurnSilently("trigger-message")
	release()
	select {
	case <-turnDone:
	case <-time.After(time.Second):
		t.Fatal("recalled prepared-card turn did not finish")
	}
	_, _, deleted := p.terminalSnapshot()
	if !reflect.DeepEqual(deleted, []any{"handle-1", "handle-2"}) {
		t.Fatalf("recall left a prepared card visible: deleted=%v", deleted)
	}
	for _, entry := range session.GetHistory(0) {
		if entry.Role == "assistant" {
			t.Fatalf("recalled prepared answer entered history: %+v", entry)
		}
	}
}
