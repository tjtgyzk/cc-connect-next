package codex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/timmyagentic/cc-connect-next/core"
)

func TestParseTodSSOConfig(t *testing.T) {
	cfg := parseTodSSOConfig(map[string]any{
		"enabled":     true,
		"binary":      "/opt/bin/tod-sso",
		"login_url":   "https://example.test/login",
		"cache_dir":   "~/cache/tod-sso",
		"timeout":     "90s",
		"force_login": true,
		"platforms":   []any{"feishu", "Lark"},
	})
	if !cfg.enabled || cfg.binary != "/opt/bin/tod-sso" || cfg.loginURL != "https://example.test/login" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.timeout != 90*time.Second {
		t.Fatalf("timeout = %s, want 90s", cfg.timeout)
	}
	if !cfg.forceLogin {
		t.Fatal("force_login was not parsed")
	}
	if !strings.HasSuffix(cfg.cacheDir, "/cache/tod-sso") {
		t.Fatalf("cache_dir = %q", cfg.cacheDir)
	}
	if !cfg.platforms["feishu"] || !cfg.platforms["lark"] {
		t.Fatalf("platforms = %#v", cfg.platforms)
	}
}

func TestParseTodSSOConfigDefaultsDisabled(t *testing.T) {
	cfg := parseTodSSOConfig(nil)
	if cfg.enabled {
		t.Fatal("tod_sso must be opt-in")
	}
	if cfg.binary != defaultTodSSOBinary || cfg.timeout != defaultTodSSOTimeout {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if !cfg.forceLogin {
		t.Fatal("force_login must default to true so users get their own token")
	}
}

func TestNew_ParsesTodSSOOptions(t *testing.T) {
	a, err := New(map[string]any{
		"work_dir": t.TempDir(),
		"cmd":      "go",
		"tod_sso": map[string]any{
			"enabled":   true,
			"cache_dir": t.TempDir(),
			"platforms": []any{"feishu"},
		},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	agent := a.(*Agent)
	if agent.todSSO == nil {
		t.Fatal("tod_sso options were not wired into the agent")
	}
	if !strings.HasSuffix(agent.todSSO.cfg.binary, "tod-sso") {
		t.Fatalf("binary = %q", agent.todSSO.cfg.binary)
	}
	if !agent.todSSO.cfg.platforms["feishu"] {
		t.Fatalf("platforms = %#v", agent.todSSO.cfg.platforms)
	}
}

func TestNew_TodSSOStripsAmbientCredentials(t *testing.T) {
	for _, name := range []string{"MULTICA_SSO_TOKEN", "NIO_SSO_TOKEN", "NIO_TOD_SSO_TOKEN"} {
		t.Setenv(name, "developer-token")
	}
	if _, err := New(map[string]any{
		"work_dir": t.TempDir(),
		"cmd":      "go",
		"tod_sso": map[string]any{
			"enabled":   true,
			"cache_dir": t.TempDir(),
		},
	}); err != nil {
		t.Fatalf("New() error: %v", err)
	}
	for _, name := range []string{"MULTICA_SSO_TOKEN", "NIO_SSO_TOKEN", "NIO_TOD_SSO_TOKEN"} {
		if got := os.Getenv(name); got != "" {
			t.Fatalf("%s leaked into the agent environment: %q", name, got)
		}
	}
}

func TestParseTokenEnv(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"plain", "NIO_TOD_SSO_TOKEN=abc", "abc"},
		{"export", "export NIO_TOD_SSO_TOKEN=abc\n", "abc"},
		{"single-quoted", "export NIO_TOD_SSO_TOKEN='abc.def'\n", "abc.def"},
		{"double-quoted", `export NIO_TOD_SSO_TOKEN="abc"` + "\n", "abc"},
		{"missing", "export OTHER=1\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseTokenEnv(tc.content); got != tc.want {
				t.Fatalf("parseTokenEnv() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTodSSOManagerCacheRoundTrip(t *testing.T) {
	cacheDir := t.TempDir()
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: cacheDir},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          time.Now,
	}
	m.storeToken("ou_user_1", "token-1")

	path := m.cachePath("ou_user_1")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("cache file missing: %v", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("cache mode = %o, want 600", info.Mode().Perm())
	}

	fresh := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: cacheDir},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          time.Now,
	}
	if got := fresh.cachedToken("ou_user_1"); got != "token-1" {
		t.Fatalf("cachedToken() = %q", got)
	}
	if got := fresh.cachedToken("ou_other"); got != "" {
		t.Fatalf("unexpected token for other user: %q", got)
	}
}

func TestTodSSOManagerUsesCacheWithoutLogin(t *testing.T) {
	cacheDir := t.TempDir()
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: cacheDir, binary: "/nonexistent/tod-sso"},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          time.Now,
	}
	m.storeToken("ou_a", "local-token")
	token, err := m.TokenForUser(context.Background(), core.SessionUserContext{UserID: "ou_a", Platform: "feishu"}, nil)
	if err != nil {
		t.Fatalf("TokenForUser() error = %v", err)
	}
	if token != "local-token" {
		t.Fatalf("token = %q", token)
	}
}

func TestTodSSOManagerExpiredCacheIsMiss(t *testing.T) {
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: t.TempDir()},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          func() time.Time { return time.Unix(1000, 0) },
	}
	m.storeToken("ou_a", fakeJWT(900))
	if got := m.cachedToken("ou_a"); got != "" {
		t.Fatalf("expired token was returned: %q", got)
	}
}

func TestTokenExpired(t *testing.T) {
	m := &todSSOManager{now: func() time.Time { return time.Unix(1000, 0) }}
	if m.tokenExpired(fakeJWT(2000)) {
		t.Fatal("valid token reported as expired")
	}
	if !m.tokenExpired(fakeJWT(900)) {
		t.Fatal("expired token reported as valid")
	}
	if m.tokenExpired("opaque-token") {
		t.Fatal("opaque token must not be treated as expired")
	}
}

func TestTodSSOManagerLoginFlow(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake tod-sso shell script is POSIX-only")
	}
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "env")
	credentialFile := filepath.Join(dir, "credentials.json")
	argsFile := filepath.Join(dir, "args")
	script := filepath.Join(dir, "tod-sso")
	body := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > " + shellQuote(argsFile) + "\n" +
		"echo 'Open this URL in your browser to sign in:' >&2\n" +
		"echo 'https://queqiao.example.test/remote?session_id=abc' >&2\n" +
		"sleep 0.3\n" +
		"printf \"export NIO_TOD_SSO_TOKEN='fresh-token'\\n\" > " + shellQuote(tokenFile) + "\n" +
		"printf '{\"access_token\":\"fresh-token\",\"refresh_token\":\"fresh-refresh\",\"expires_at\":\"2030-01-01T00:00:00Z\"}\\n' > " + shellQuote(credentialFile) + "\n" +
		"exit 0\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}

	m := &todSSOManager{
		cfg: todSSOConfig{
			enabled:    true,
			binary:     script,
			cacheDir:   filepath.Join(dir, "cache"),
			timeout:    5 * time.Second,
			forceLogin: true,
			platforms:  map[string]bool{"feishu": true},
		},
		cache:               make(map[string]todSSOCredential),
		cacheModTime:        make(map[string]time.Time),
		localTokenFile:      func() string { return tokenFile },
		localCredentialFile: func() string { return credentialFile },
		now:                 time.Now,
	}

	var mu sync.Mutex
	var notified []string
	notify := func(key core.MsgKey, _ ...interface{}) {
		mu.Lock()
		notified = append(notified, string(key))
		mu.Unlock()
	}
	token, err := m.TokenForUser(context.Background(), core.SessionUserContext{UserID: "ou_b", Platform: "feishu"}, notify)
	if err != nil {
		t.Fatalf("TokenForUser() error = %v", err)
	}
	if token != "fresh-token" {
		t.Fatalf("token = %q", token)
	}
	mu.Lock()
	got := strings.Join(notified, ",")
	mu.Unlock()
	want := string(core.MsgTODSSOLoginRequired) + "," + string(core.MsgTODSSOLoggedIn)
	if got != want {
		t.Fatalf("notifications = %q, want %q", got, want)
	}
	if got := m.cachedToken("ou_b"); got != "fresh-token" {
		t.Fatalf("token was not cached: %q", got)
	}
	cached, ok := m.loadCredential("ou_b")
	if !ok {
		t.Fatal("credential was not cached")
	}
	if cached.RefreshToken != "fresh-refresh" {
		t.Fatalf("refresh token = %q", cached.RefreshToken)
	}
	if cached.ExpiresAt != "2030-01-01T00:00:00Z" {
		t.Fatalf("expires_at = %q", cached.ExpiresAt)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	argList := strings.Fields(string(args))
	if !containsArg(argList, "--force") || !containsArg(argList, "--json") {
		t.Fatalf("login args = %#v, want --force and --json", argList)
	}
}

func TestTodSSOManagerCacheFileIsPerUserAndReadable(t *testing.T) {
	cacheDir := t.TempDir()
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: cacheDir},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          time.Now,
	}
	m.storeToken("ou_readable_user", "token-a")
	m.storeToken("ou_second_user", "token-b")

	a := m.cachePath("ou_readable_user")
	b := m.cachePath("ou_second_user")
	if a == b {
		t.Fatal("different users share one cache file")
	}
	if filepath.Base(a) != "ou_readable_user.json" {
		t.Fatalf("cache file = %q, want readable user name", filepath.Base(a))
	}
	if filepath.Dir(a) != cacheDir {
		t.Fatalf("cache dir = %q", filepath.Dir(a))
	}
	raw, err := os.ReadFile(a)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"token": "token-a"`) {
		t.Fatalf("cache file is not human-editable JSON: %s", raw)
	}
}

func TestExtractLoginURL(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{"Open this URL in your browser to sign in:", ""},
		{"https://queqiao.example.test/remote?session_id=abc", "https://queqiao.example.test/remote?session_id=abc"},
		{"Queqiao TOD SSO login URL: https://queqiao.example.test/login?code=abc", "https://queqiao.example.test/login?code=abc"},
		{`{"error":"context deadline exceeded","exit_code":1}`, ""},
	}
	for _, tc := range cases {
		if got := extractLoginURL(tc.line); got != tc.want {
			t.Fatalf("extractLoginURL(%q) = %q, want %q", tc.line, got, tc.want)
		}
	}
}

func TestTodSSOManagerPendingLoginIsReused(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake tod-sso shell script is POSIX-only")
	}
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "env")
	credentialFile := filepath.Join(dir, "credentials.json")
	runsFile := filepath.Join(dir, "runs")
	script := filepath.Join(dir, "tod-sso")
	body := "#!/bin/sh\n" +
		"echo run >> " + shellQuote(runsFile) + "\n" +
		"echo 'Open this URL in your browser to sign in:' >&2\n" +
		"echo 'https://queqiao.example.test/remote?session_id=abc' >&2\n" +
		"sleep 1\n" +
		"printf \"export NIO_TOD_SSO_TOKEN='fresh-token'\\n\" > " + shellQuote(tokenFile) + "\n" +
		"printf '{\"access_token\":\"fresh-token\"}\\n' > " + shellQuote(credentialFile) + "\n" +
		"exit 0\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	m := &todSSOManager{
		cfg: todSSOConfig{
			enabled: true, binary: script, cacheDir: filepath.Join(dir, "cache"),
			timeout: 5 * time.Second, forceLogin: true, platforms: map[string]bool{"feishu": true},
		},
		cache:               make(map[string]todSSOCredential),
		cacheModTime:        make(map[string]time.Time),
		pending:             make(map[string]*pendingLogin),
		localTokenFile:      func() string { return tokenFile },
		localCredentialFile: func() string { return credentialFile },
		now:                 time.Now,
	}
	user := core.SessionUserContext{UserID: "ou_dedup", Platform: "feishu"}
	type result struct {
		token string
		err   error
	}
	results := make(chan result, 2)
	go func() {
		token, err := m.TokenForUser(context.Background(), user, nil)
		results <- result{token, err}
	}()
	time.Sleep(100 * time.Millisecond)
	go func() {
		token, err := m.TokenForUser(context.Background(), user, nil)
		results <- result{token, err}
	}()
	for i := 0; i < 2; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("call %d error = %v", i+1, res.err)
		}
		if res.token != "fresh-token" {
			t.Fatalf("call %d token = %q", i+1, res.token)
		}
	}
	runs, err := os.ReadFile(runsFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(runs), "run"); got != 1 {
		t.Fatalf("tod-sso login ran %d times, want 1", got)
	}
}

func TestTodSSOLoginEnvSuppressesBrowserOpen(t *testing.T) {
	m := &todSSOManager{cfg: todSSOConfig{enabled: true, cacheDir: t.TempDir()}}
	env, cleanup := m.loginCommandEnv()
	defer cleanup()

	path := ""
	for _, entry := range env {
		if strings.HasPrefix(entry, "PATH=") {
			path = strings.TrimPrefix(entry, "PATH=")
		}
	}
	if path == "" {
		t.Fatal("PATH missing from login environment")
	}
	dir := strings.Split(path, string(os.PathListSeparator))[0]
	for _, name := range []string{"open", "xdg-open"} {
		shim := filepath.Join(dir, name)
		info, err := os.Stat(shim)
		if err != nil {
			t.Fatalf("%s shim missing: %v", name, err)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm()&0o100 == 0 {
			t.Fatalf("%s shim is not executable: %o", name, info.Mode().Perm())
		}
	}
}

func TestTodSSOManagerCacheFileSanitizesUnsafeUserID(t *testing.T) {
	m := &todSSOManager{cfg: todSSOConfig{enabled: true, cacheDir: t.TempDir()}}
	path := m.cachePath("../evil/user")
	if filepath.Dir(path) != m.cfg.cacheDir {
		t.Fatalf("cache path escaped cache dir: %q", path)
	}
	if strings.Contains(filepath.Base(path), "/") {
		t.Fatalf("cache file name contains separator: %q", path)
	}
	if !strings.Contains(filepath.Base(path), ".._evil_user-") {
		t.Fatalf("cache file name = %q", filepath.Base(path))
	}
}

func TestTodSSOManagerCacheManualEditIsPickedUp(t *testing.T) {
	cacheDir := t.TempDir()
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: cacheDir},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          time.Now,
	}
	m.storeToken("ou_edit", "old-token")
	if got := m.cachedToken("ou_edit"); got != "old-token" {
		t.Fatalf("cachedToken() = %q", got)
	}

	path := m.cachePath("ou_edit")
	edited := todSSOCredential{UserID: "ou_edit", Token: "edited-token"}
	payload, _ := json.Marshal(edited)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	// Ensure mtime advances even on coarse filesystems.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if got := m.cachedToken("ou_edit"); got != "edited-token" {
		t.Fatalf("manual edit was ignored: %q", got)
	}
}

func TestTodSSOManagerReadsLegacyHashedCache(t *testing.T) {
	cacheDir := t.TempDir()
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: cacheDir},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          time.Now,
	}
	payload, _ := json.Marshal(todSSOCredential{UserID: "ou_legacy", Token: "legacy-token"})
	if err := os.WriteFile(m.legacyCachePath("ou_legacy"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := m.cachedToken("ou_legacy"); got != "legacy-token" {
		t.Fatalf("legacy cache was not read: %q", got)
	}
}

func TestTodSSOManagerDeletedCacheForcesRelogin(t *testing.T) {
	cacheDir := t.TempDir()
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: cacheDir},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          time.Now,
	}
	m.storeToken("ou_delete", "token")
	if got := m.cachedToken("ou_delete"); got != "token" {
		t.Fatalf("cachedToken() = %q", got)
	}
	if err := os.Remove(m.cachePath("ou_delete")); err != nil {
		t.Fatal(err)
	}
	if got := m.cachedToken("ou_delete"); got != "" {
		t.Fatalf("deleted cache was still served from memory: %q", got)
	}
}

func TestReadLocalCredentialIgnoresStaleRefreshToken(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "env")
	credentialFile := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(tokenFile, []byte("NIO_TOD_SSO_TOKEN=new-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credentialFile, []byte(`{"access_token":"old-token","refresh_token":"old-refresh"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	m := &todSSOManager{
		localTokenFile:      func() string { return tokenFile },
		localCredentialFile: func() string { return credentialFile },
	}
	cred, err := m.readLocalCredential()
	if err != nil {
		t.Fatal(err)
	}
	if cred.Token != "new-token" {
		t.Fatalf("token = %q", cred.Token)
	}
	if cred.RefreshToken != "" {
		t.Fatalf("stale refresh token was accepted: %q", cred.RefreshToken)
	}
	if cred.accessTokenMatches {
		t.Fatal("mismatched credential files reported as in sync")
	}
}

func TestTodSSOManagerExpiresAtIsHonored(t *testing.T) {
	expiresAt := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: t.TempDir()},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          func() time.Time { return expiresAt.Add(time.Hour) },
	}
	m.storeCredential(todSSOCredential{
		UserID:    "ou_a",
		Token:     "opaque-token",
		ExpiresAt: expiresAt.Format(time.RFC3339),
	})
	if got := m.cachedToken("ou_a"); got != "" {
		t.Fatalf("credential past expires_at was returned: %q", got)
	}
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func TestBootstrapSessionUserBuildsEnv(t *testing.T) {
	cacheDir := t.TempDir()
	m := &todSSOManager{
		cfg:          todSSOConfig{enabled: true, cacheDir: cacheDir, platforms: map[string]bool{"feishu": true}},
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		now:          time.Now,
	}
	m.storeToken("ou_c", "cached-token")
	a := &Agent{todSSO: m}
	env, err := a.BootstrapSessionUser(context.Background(), core.SessionUserContext{
		Platform: "feishu", UserID: "ou_c", SessionKey: "feishu:chat:ou_c",
	}, nil)
	if err != nil {
		t.Fatalf("BootstrapSessionUser() error = %v", err)
	}
	if len(env) != 1 || env[0] != "MULTICA_SSO_TOKEN=cached-token" {
		t.Fatalf("env = %#v", env)
	}
}

func TestBootstrapSessionUserSkipsOtherPlatforms(t *testing.T) {
	m := &todSSOManager{
		cfg:   todSSOConfig{enabled: true, cacheDir: t.TempDir(), platforms: map[string]bool{"feishu": true}},
		cache: map[string]todSSOCredential{"ou_c": {UserID: "ou_c", Token: "cached-token"}},
	}
	a := &Agent{todSSO: m}
	env, err := a.BootstrapSessionUser(context.Background(), core.SessionUserContext{
		Platform: "telegram", UserID: "42",
	}, nil)
	if err != nil || len(env) != 0 {
		t.Fatalf("env = %#v, err = %v", env, err)
	}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func fakeJWT(exp int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp)))
	return header + "." + payload + ".sig"
}
