package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

type bootstrapRecordingAgent struct {
	stubAgent

	mu       sync.Mutex
	calls    int
	lastUser SessionUserContext
	envOut   []string
	err      error
	env      []string
}

func (a *bootstrapRecordingAgent) SetSessionEnv(env []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.env = append([]string(nil), env...)
}

func (a *bootstrapRecordingAgent) BootstrapSessionUser(_ context.Context, user SessionUserContext, notify func(MsgKey, ...interface{})) ([]string, error) {
	a.mu.Lock()
	a.calls++
	a.lastUser = user
	err := a.err
	out := append([]string(nil), a.envOut...)
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if notify != nil {
		notify(MsgTODSSOLoginRequired, "https://example.test/login")
	}
	return out, nil
}

func (a *bootstrapRecordingAgent) snapshot() (int, SessionUserContext, []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls, a.lastUser, append([]string(nil), a.env...)
}

func TestPrepareSessionUserBootstrap_NewSessionInjectsEnv(t *testing.T) {
	agent := &bootstrapRecordingAgent{envOut: []string{"MULTICA_SSO_TOKEN=abc"}}
	platform := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	e.SetDataDir(t.TempDir())
	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("feishu:chat:ou_user")
	msg := &Message{Platform: "feishu", SessionKey: "feishu:chat:ou_user", UserID: "ou_user", UserName: "Phil"}

	env, ok := e.prepareSessionUserBootstrap(platform, msg, agent, msg.SessionKey, session)
	if !ok {
		t.Fatal("prepareSessionUserBootstrap reported failure")
	}
	if len(env) != 1 || env[0] != "MULTICA_SSO_TOKEN=abc" {
		t.Fatalf("env = %#v", env)
	}
	calls, user, _ := agent.snapshot()
	if calls != 1 || user.UserID != "ou_user" || user.UserName != "Phil" || user.Platform != "feishu" {
		t.Fatalf("calls = %d, user = %+v", calls, user)
	}
	sent := strings.Join(platform.getSent(), "\n")
	if !strings.Contains(sent, "https://example.test/login") {
		t.Fatalf("login link was not sent to the user: %q", sent)
	}
}

type directUserPlatform struct {
	stubPlatformEngine
	directUsers []string
	directText  []string
	sendErr     error
}

func (p *directUserPlatform) Send(ctx context.Context, replyCtx any, content string) error {
	if p.sendErr != nil {
		return p.sendErr
	}
	return p.stubPlatformEngine.Send(ctx, replyCtx, content)
}

func (p *directUserPlatform) SendDirectUser(_ context.Context, userID, content string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.directUsers = append(p.directUsers, userID)
	p.directText = append(p.directText, content)
	return nil
}

func TestPrepareSessionUserBootstrap_NotifiesChatFirst(t *testing.T) {
	agent := &bootstrapRecordingAgent{}
	platform := &directUserPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	sessions := NewSessionManager("")
	key := msgSessionKey()
	session := sessions.GetOrCreateActive(key)
	msg := &Message{Platform: "feishu", SessionKey: key, UserID: "ou_user"}

	if _, ok := e.prepareSessionUserBootstrap(platform, msg, agent, key, session); !ok {
		t.Fatal("prepareSessionUserBootstrap reported failure")
	}
	if sent := strings.Join(platform.getSent(), "\n"); !strings.Contains(sent, "https://example.test/login") {
		t.Fatalf("login link was not sent to the chat: %q", sent)
	}
	if len(platform.directUsers) != 0 {
		t.Fatalf("unexpected direct messages: %#v", platform.directText)
	}
}

func TestPrepareSessionUserBootstrap_FallsBackToDirectMessage(t *testing.T) {
	agent := &bootstrapRecordingAgent{}
	platform := &directUserPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		sendErr:            errors.New("chat send failed"),
	}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	sessions := NewSessionManager("")
	key := msgSessionKey()
	session := sessions.GetOrCreateActive(key)
	msg := &Message{Platform: "feishu", SessionKey: key, UserID: "ou_user"}

	if _, ok := e.prepareSessionUserBootstrap(platform, msg, agent, key, session); !ok {
		t.Fatal("prepareSessionUserBootstrap reported failure")
	}
	if len(platform.directUsers) != 1 || platform.directUsers[0] != "ou_user" {
		t.Fatalf("direct users = %#v", platform.directUsers)
	}
	if !strings.Contains(strings.Join(platform.directText, "\n"), "https://example.test/login") {
		t.Fatalf("login link was not delivered as fallback: %#v", platform.directText)
	}
}

func TestPrepareSessionUserBootstrap_SkipsWhenProcessIsReused(t *testing.T) {
	agent := &bootstrapRecordingAgent{envOut: []string{"MULTICA_SSO_TOKEN=abc"}}
	platform := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	e.SetDataDir(t.TempDir())
	sessions := NewSessionManager("")
	key := msgSessionKey()
	session := sessions.GetOrCreateActive(key)
	session.SetAgentSessionID("stub-session", agent.Name())

	e.getOrCreateInteractiveStateWith(key, platform, "ctx", session, sessions, nil, "", "hello")
	platform.clearSent()

	msg := &Message{Platform: "feishu", SessionKey: key, UserID: "ou_user"}
	env, ok := e.prepareSessionUserBootstrap(platform, msg, agent, msg.SessionKey, session)
	if !ok {
		t.Fatal("prepareSessionUserBootstrap reported failure")
	}
	if len(env) != 0 {
		t.Fatalf("env = %#v, want nil", env)
	}
	if calls, _, _ := agent.snapshot(); calls != 0 {
		t.Fatalf("bootstrap called %d times for a reused session", calls)
	}
	if sent := platform.getSent(); len(sent) != 0 {
		t.Fatalf("unexpected messages: %#v", sent)
	}
}

func TestPrepareSessionUserBootstrap_FailureAbortsAndReplies(t *testing.T) {
	agent := &bootstrapRecordingAgent{err: errors.New("login timed out")}
	platform := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	sessions := NewSessionManager("")
	key := msgSessionKey()
	session := sessions.GetOrCreateActive(key)
	msg := &Message{Platform: "feishu", SessionKey: key, UserID: "ou_user"}

	_, ok := e.prepareSessionUserBootstrap(platform, msg, agent, msg.SessionKey, session)
	if ok {
		t.Fatal("expected failure")
	}
	sent := strings.Join(platform.getSent(), "\n")
	if !strings.Contains(sent, "login timed out") {
		t.Fatalf("failure was not reported: %q", sent)
	}
}

func TestPrepareSessionUserBootstrap_PendingEndsTurnQuietly(t *testing.T) {
	agent := &bootstrapRecordingAgent{err: ErrSessionUserBootstrapPending}
	platform := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	sessions := NewSessionManager("")
	key := msgSessionKey()
	session := sessions.GetOrCreateActive(key)
	msg := &Message{Platform: "feishu", SessionKey: key, UserID: "ou_user"}

	_, ok := e.prepareSessionUserBootstrap(platform, msg, agent, msg.SessionKey, session)
	if ok {
		t.Fatal("pending bootstrap must abort the turn")
	}
	if sent := platform.getSent(); len(sent) != 0 {
		t.Fatalf("pending bootstrap must not send a failure message: %#v", sent)
	}
}

func TestGetOrCreateInteractiveStateWithEnv_AppendsBootstrapEnvLast(t *testing.T) {
	agent := &bootstrapRecordingAgent{}
	platform := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish)
	e.SetDataDir(t.TempDir())
	sessions := NewSessionManager("")
	key := msgSessionKey()
	session := sessions.GetOrCreateActive(key)

	e.getOrCreateInteractiveStateWithEnv(key, platform, "ctx", session, sessions, nil, "", "hello",
		[]string{"MULTICA_SSO_TOKEN=abc", "PATH=/bootstrap"})

	_, _, env := agent.snapshot()
	want := []string{"MULTICA_SSO_TOKEN=abc", "PATH=/bootstrap"}
	if len(env) < len(want) {
		t.Fatalf("env = %#v", env)
	}
	tail := env[len(env)-len(want):]
	for i := range want {
		if tail[i] != want[i] {
			t.Fatalf("env tail = %#v, want %#v (full env %#v)", tail, want, env)
		}
	}
}

func msgSessionKey() string { return "feishu:chat:ou_user" }
