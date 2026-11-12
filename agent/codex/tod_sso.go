package codex

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/timmyagentic/cc-connect-next/core"
)

const (
	todSSOTokenEnv       = "MULTICA_SSO_TOKEN"
	defaultTodSSOBinary  = "tod-sso"
	defaultTodSSOTimeout = 5 * time.Minute
	todSSOLoginURLPrefix = "Queqiao TOD SSO login URL:"
)

// todSSOConfig controls per-user TOD SSO credential injection for Codex.
//
//	toml:
//	[projects.agent.options.tod_sso]
//	enabled = true
//	binary = "tod-sso"
//	login_url = "https://queqiao.nioint.com/context-nio/auth/tod-sso"
//	cache_dir = "~/.cc-connect-next/tod-sso"
//	timeout = "5m"
//	force_login = true
//	platforms = ["feishu"]
type todSSOConfig struct {
	enabled    bool
	binary     string
	cacheDir   string
	loginURL   string
	timeout    time.Duration
	forceLogin bool
	platforms  map[string]bool
}

type todSSOCredential struct {
	UserID       string    `json:"user_id"`
	UserName     string    `json:"user_name,omitempty"`
	Platform     string    `json:"platform,omitempty"`
	Token        string    `json:"token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	ExpiresAt    string    `json:"expires_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`

	// accessTokenMatches reports whether tod-sso's env file and
	// credentials.json described the same login when they were read.
	accessTokenMatches bool
}

// todSSOManager resolves and caches TOD SSO tokens per messaging user.
//
// tod-sso itself only persists one token for the local OS user. This manager
// keeps the mapping from platform user ID to that token so a later session for
// the same user can start Codex without another interactive login.
type todSSOManager struct {
	cfg todSSOConfig

	// Overridable for tests.
	localTokenFile      func() string
	localCredentialFile func() string
	now                 func() time.Time

	mu           sync.Mutex
	cache        map[string]todSSOCredential
	cacheModTime map[string]time.Time
	pending      map[string]*pendingLogin

	// loginMu serializes interactive logins. Two chats must not race to
	// overwrite the same tod-sso credential file.
	loginMu sync.Mutex
}

// pendingLogin is one in-flight browser sign-in. The Agent process must not
// start until the user finishes the browser step and sends a new message, so
// the login runs in the background while the turn ends.
type pendingLogin struct {
	done     chan struct{}
	urlReady chan struct{}
	err      error
}

func newTodSSOManager(raw any) *todSSOManager {
	cfg := parseTodSSOConfig(raw)
	if !cfg.enabled {
		return nil
	}
	cfg.binary = resolveTodSSOBinary(cfg.binary)
	if _, err := exec.LookPath(cfg.binary); err != nil {
		// Keep the manager: the binary may be installed later or the configured
		// path may be provided by a wrapper. The login attempt reports the
		// actionable error to the user.
		slog.Warn("codex: tod_sso enabled but binary not found in PATH",
			"binary", cfg.binary, "error", err)
	}
	// The daemon may have inherited a developer's SSO token from the shell that
	// started it. Per-user injection must be the only credential source for
	// Agent processes, so drop the ambient token instead of leaking one user's
	// identity into every session.
	for _, name := range []string{"MULTICA_SSO_TOKEN", "NIO_SSO_TOKEN", "NIO_TOD_SSO_TOKEN"} {
		_ = os.Unsetenv(name)
	}
	return &todSSOManager{
		cfg:          cfg,
		cache:        make(map[string]todSSOCredential),
		cacheModTime: make(map[string]time.Time),
		pending:      make(map[string]*pendingLogin),
		now:          time.Now,
	}
}

// resolveTodSSOBinary keeps the daemon's often-minimal PATH from hiding the
// standard Queqiao CLI location.
func resolveTodSSOBinary(binary string) string {
	binary = strings.TrimSpace(binary)
	if binary == "" {
		binary = defaultTodSSOBinary
	}
	if strings.ContainsRune(binary, os.PathSeparator) {
		return binary
	}
	if resolved, err := exec.LookPath(binary); err == nil {
		return resolved
	}
	home, _ := os.UserHomeDir()
	candidates := []string{
		filepath.Join(home, ".queqiao", "bin", "tod-sso"),
		"/usr/local/bin/tod-sso",
		"/opt/homebrew/bin/tod-sso",
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return candidate
		}
	}
	return binary
}

func parseTodSSOConfig(raw any) todSSOConfig {
	home, _ := os.UserHomeDir()
	cfg := todSSOConfig{
		binary:     defaultTodSSOBinary,
		cacheDir:   filepath.Join(home, ".cc-connect-next", "tod-sso"),
		timeout:    defaultTodSSOTimeout,
		forceLogin: true,
		platforms:  map[string]bool{"feishu": true},
	}
	ssoOpts, _ := raw.(map[string]any)
	if ssoOpts == nil {
		return cfg
	}
	if v, ok := ssoOpts["enabled"].(bool); ok {
		cfg.enabled = v
	}
	if v, ok := ssoOpts["binary"].(string); ok && strings.TrimSpace(v) != "" {
		cfg.binary = strings.TrimSpace(v)
	}
	if v, ok := ssoOpts["login_url"].(string); ok && strings.TrimSpace(v) != "" {
		cfg.loginURL = strings.TrimSpace(v)
	}
	if v, ok := ssoOpts["cache_dir"].(string); ok && strings.TrimSpace(v) != "" {
		cfg.cacheDir = expandHome(strings.TrimSpace(v), home)
	}
	if v, ok := ssoOpts["force_login"].(bool); ok {
		cfg.forceLogin = v
	}
	switch v := ssoOpts["timeout"].(type) {
	case string:
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil && d > 0 {
			cfg.timeout = d
		}
	case int64:
		if v > 0 {
			cfg.timeout = time.Duration(v) * time.Second
		}
	case int:
		if v > 0 {
			cfg.timeout = time.Duration(v) * time.Second
		}
	}
	if platforms := stringListOption(ssoOpts["platforms"]); len(platforms) > 0 {
		cfg.platforms = make(map[string]bool, len(platforms))
		for _, platform := range platforms {
			cfg.platforms[strings.ToLower(strings.TrimSpace(platform))] = true
		}
	}
	return cfg
}

func expandHome(path, home string) string {
	if home == "" {
		return path
	}
	if path == "~" {
		return home
	}
	if strings.HasPrefix(path, "~/") {
		return filepath.Join(home, path[2:])
	}
	return path
}

func stringListOption(raw any) []string {
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func (m *todSSOManager) enabledFor(platform string) bool {
	if m == nil || !m.cfg.enabled {
		return false
	}
	if len(m.cfg.platforms) == 0 {
		return true
	}
	return m.cfg.platforms[strings.ToLower(strings.TrimSpace(platform))]
}

// BootstrapSessionUser implements core.SessionUserBootstrapper.
func (a *Agent) BootstrapSessionUser(ctx context.Context, user core.SessionUserContext, notify func(core.MsgKey, ...interface{})) ([]string, error) {
	a.mu.RLock()
	manager := a.todSSO
	a.mu.RUnlock()
	if manager == nil || !manager.enabledFor(user.Platform) {
		return nil, nil
	}
	if strings.TrimSpace(user.UserID) == "" {
		return nil, nil
	}
	token, err := manager.TokenForUser(ctx, user, notify)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, nil
	}
	return []string{todSSOTokenEnv + "=" + token}, nil
}

// TokenForUser returns a cached token, or starts a forced browser login and
// waits for it to finish. The caller's turn stays open while the user signs in,
// then continues automatically with the same message.
func (m *todSSOManager) TokenForUser(ctx context.Context, user core.SessionUserContext, notify func(core.MsgKey, ...interface{})) (string, error) {
	if token := m.cachedToken(user.UserID); token != "" {
		return token, nil
	}

	pending := m.pendingFor(user.UserID)
	if pending == nil {
		pending = m.startLogin(user, notify)
	}

	// Wait until the sign-in link has been delivered, so the user always has
	// something actionable while the turn waits.
	select {
	case <-pending.urlReady:
	case <-pending.done:
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(30 * time.Second):
		return "", errors.New("tod-sso login did not report a sign-in URL")
	}

	// Block until the browser login finishes or times out. The Engine keeps the
	// user's original message and resumes this turn automatically afterwards.
	select {
	case <-pending.done:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if token := m.cachedToken(user.UserID); token != "" {
		return token, nil
	}
	if pending.err != nil {
		return "", pending.err
	}
	return "", errors.New("tod-sso login finished without a credential")
}

func (m *todSSOManager) pendingFor(userID string) *pendingLogin {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pending[userID]
}

// startLogin runs the interactive login in the background. The browser login
// may take minutes, so it must outlive the chat turn that triggered it.
func (m *todSSOManager) startLogin(user core.SessionUserContext, notify func(core.MsgKey, ...interface{})) *pendingLogin {
	pending := &pendingLogin{
		done:     make(chan struct{}),
		urlReady: make(chan struct{}),
	}
	m.mu.Lock()
	if m.pending == nil {
		m.pending = make(map[string]*pendingLogin)
	}
	if existing := m.pending[user.UserID]; existing != nil {
		m.mu.Unlock()
		return existing
	}
	m.pending[user.UserID] = pending
	m.mu.Unlock()

	go func() {
		defer func() {
			m.mu.Lock()
			if m.pending[user.UserID] == pending {
				delete(m.pending, user.UserID)
			}
			m.mu.Unlock()
			close(pending.done)
		}()

		// tod-sso persists one credential for the local OS user, so logins from
		// different chat users must not run at the same time.
		m.loginMu.Lock()
		defer m.loginMu.Unlock()

		loginCtx, cancel := context.WithTimeout(context.Background(), m.loginTimeout()+30*time.Second)
		defer cancel()

		onURL := func(url string) {
			close(pending.urlReady)
			if notify != nil {
				notify(core.MsgTODSSOLoginRequired, url)
			}
		}
		cred, err := m.login(loginCtx, onURL)
		if err != nil {
			pending.err = err
			return
		}
		m.storeCredential(todSSOCredential{
			UserID:       user.UserID,
			UserName:     user.UserName,
			Platform:     user.Platform,
			Token:        cred.Token,
			RefreshToken: cred.RefreshToken,
			ExpiresAt:    cred.ExpiresAt,
		})
		if notify != nil {
			notify(core.MsgTODSSOLoggedIn)
		}
	}()
	return pending
}

func (m *todSSOManager) loginTimeout() time.Duration {
	if m.cfg.timeout > 0 {
		return m.cfg.timeout
	}
	return defaultTodSSOTimeout
}

func (m *todSSOManager) login(ctx context.Context, onURL func(string)) (todSSOCredential, error) {
	timeout := m.loginTimeout()
	loginCtx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
	defer cancel()

	// --force guarantees a fresh credential for THIS user instead of reusing
	// whatever credential the last user left in tod-sso's single local store.
	// --json keeps machine output structured; the credential itself is read
	// from tod-sso's private credential files, never from stdout.
	args := []string{"login"}
	if m.cfg.forceLogin {
		args = append(args, "--force")
	}
	args = append(args, "--json", "-timeout", timeout.String())
	if loginURL := m.resolveLoginURL(); loginURL != "" {
		args = append(args, "-login-url", loginURL)
	}

	env, cleanup := m.loginCommandEnv()
	defer cleanup()

	cmd := exec.CommandContext(loginCtx, m.cfg.binary, args...)
	cmd.Env = env
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return todSSOCredential{}, fmt.Errorf("tod-sso login: stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return todSSOCredential{}, fmt.Errorf("tod-sso login: stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return todSSOCredential{}, fmt.Errorf("start tod-sso login: %w", err)
	}

	tail := &tailBuffer{limit: 4096}
	var urlOnce sync.Once
	handleURL := func(url string) {
		if url == "" {
			return
		}
		urlOnce.Do(func() {
			if onURL != nil {
				onURL(url)
			}
		})
	}
	var readers sync.WaitGroup
	readers.Add(2)
	go func() {
		defer readers.Done()
		scanLoginOutput(stdout, handleURL, nil)
	}()
	go func() {
		defer readers.Done()
		scanLoginOutput(stderr, handleURL, tail)
	}()

	// Readers observe EOF when the child exits. Drain them before Wait so the
	// credential URL cannot be truncated by Wait closing the parent pipe.
	readers.Wait()
	waitErr := cmd.Wait()
	if waitErr != nil {
		if errors.Is(loginCtx.Err(), context.DeadlineExceeded) {
			return todSSOCredential{}, fmt.Errorf("tod-sso login timed out after %s", timeout)
		}
		detail := strings.TrimSpace(tail.String())
		if len(detail) > 500 {
			detail = detail[len(detail)-500:]
		}
		if detail != "" {
			return todSSOCredential{}, fmt.Errorf("tod-sso login failed: %w: %s", waitErr, detail)
		}
		return todSSOCredential{}, fmt.Errorf("tod-sso login failed: %w", waitErr)
	}

	cred, err := m.readLocalCredentialWithRetry()
	if err != nil {
		return todSSOCredential{}, err
	}
	return cred, nil
}

func scanLoginOutput(r io.Reader, onURL func(string), tail *tailBuffer) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if tail != nil {
			tail.WriteString(line + "\n")
		}
		if url := extractLoginURL(line); url != "" {
			onURL(url)
		}
	}
}

func extractLoginURL(line string) string {
	if idx := strings.Index(line, todSSOLoginURLPrefix); idx >= 0 {
		rest := strings.TrimSpace(line[idx+len(todSSOLoginURLPrefix):])
		if fields := strings.Fields(rest); len(fields) > 0 && strings.HasPrefix(fields[0], "http") {
			return fields[0]
		}
	}
	// tod-sso prints the URL on its own line after "Open this URL in your
	// browser to sign in:", so accept any standalone HTTP(S) field.
	for _, field := range strings.Fields(line) {
		field = strings.Trim(field, `"',;)]}`)
		if strings.HasPrefix(field, "https://") || strings.HasPrefix(field, "http://") {
			return field
		}
	}
	return ""
}

// withoutSSOEnv prevents a credential inherited from the daemon environment
// from being mistaken for the credential inside the tod-sso credential file.
func withoutSSOEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "MULTICA_SSO_TOKEN", "NIO_SSO_TOKEN", "NIO_TOD_SSO_TOKEN":
			continue
		}
		out = append(out, entry)
	}
	return out
}

// loginCommandEnv hides browser-launcher executables behind no-op shims.
// tod-sso opens the sign-in page automatically when one is available; the
// daemon must not open a browser on the host, because the link is delivered
// to the requesting chat user instead.
func (m *todSSOManager) loginCommandEnv() ([]string, func()) {
	env := withoutSSOEnv(os.Environ())
	dir, err := os.MkdirTemp("", "cc-connect-next-tod-sso-*")
	if err != nil {
		return env, func() {}
	}
	wrote := false
	for _, name := range []string{"open", "xdg-open", "sensible-browser", "x-www-browser", "www-browser"} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err == nil {
			wrote = true
		}
	}
	if !wrote {
		_ = os.RemoveAll(dir)
		return env, func() {}
	}
	env = setEnvEntry(env, "PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return env, func() { _ = os.RemoveAll(dir) }
}

func setEnvEntry(env []string, name, value string) []string {
	prefix := name + "="
	out := env[:0]
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			continue
		}
		out = append(out, entry)
	}
	return append(out, prefix+value)
}

// readLocalCredentialWithRetry waits briefly because tod-sso may still be
// flushing its credential files when the process exits.
func (m *todSSOManager) readLocalCredentialWithRetry() (todSSOCredential, error) {
	var lastErr error
	var last todSSOCredential
	for attempt := 0; attempt < 10; attempt++ {
		cred, err := m.readLocalCredential()
		if err == nil && strings.TrimSpace(cred.Token) != "" {
			last = cred
			if cred.accessTokenMatches {
				return cred, nil
			}
			lastErr = errors.New("tod-sso credential files are still being written")
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	if strings.TrimSpace(last.Token) != "" {
		// The access token is usable, but a refresh token from a previous
		// login must never be attached to it.
		last.RefreshToken = ""
		last.ExpiresAt = ""
		return last, nil
	}
	if lastErr == nil {
		lastErr = errors.New("token file is empty")
	}
	return todSSOCredential{}, fmt.Errorf("tod-sso login completed but no token was persisted: %w", lastErr)
}

// readLocalCredential reads the access token from tod-sso's private env file
// and the refresh token from its credentials.json. Older tod-sso releases only
// write the env file; the refresh token is then simply absent.
func (m *todSSOManager) readLocalCredential() (todSSOCredential, error) {
	path := m.tokenFilePath()
	if path == "" {
		return todSSOCredential{}, errors.New("cannot determine tod-sso credential path")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return todSSOCredential{}, err
	}
	cred := todSSOCredential{Token: parseTokenEnv(string(data)), accessTokenMatches: true}
	if credentialPath := m.credentialFilePath(); credentialPath != "" {
		if raw, readErr := os.ReadFile(credentialPath); readErr == nil {
			var stored struct {
				AccessToken  string `json:"access_token"`
				RefreshToken string `json:"refresh_token"`
				ExpiresAt    string `json:"expires_at"`
			}
			if json.Unmarshal(raw, &stored) == nil {
				if cred.Token == "" {
					cred.Token = strings.TrimSpace(stored.AccessToken)
				}
				accessToken := strings.TrimSpace(stored.AccessToken)
				cred.accessTokenMatches = accessToken == "" || accessToken == cred.Token
				if cred.accessTokenMatches {
					cred.RefreshToken = strings.TrimSpace(stored.RefreshToken)
					cred.ExpiresAt = strings.TrimSpace(stored.ExpiresAt)
				}
			}
		}
	}
	if cred.Token == "" {
		return cred, fmt.Errorf("NIO_TOD_SSO_TOKEN not found in %s", path)
	}
	return cred, nil
}

func (m *todSSOManager) tokenFilePath() string {
	if m.localTokenFile != nil {
		return m.localTokenFile()
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "nio-tod-sso", "env")
}

// credentialFilePath returns tod-sso's credentials.json, which carries the
// refresh token (the env file only carries the access token).
func (m *todSSOManager) credentialFilePath() string {
	if m.localCredentialFile != nil {
		return m.localCredentialFile()
	}
	path := m.tokenFilePath()
	if path == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(path), "credentials.json")
}

// resolveLoginURL prefers the explicit option and falls back to the login URL
// saved by a previous `tod-sso init`, so headless daemons can start the remote
// browser flow without an interactive prompt.
func (m *todSSOManager) resolveLoginURL() string {
	if m.cfg.loginURL != "" {
		return m.cfg.loginURL
	}
	path := m.tokenFilePath()
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(path), "config.json"))
	if err != nil {
		return ""
	}
	var cfg struct {
		LoginURL string `json:"login_url"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return ""
	}
	return strings.TrimSpace(cfg.LoginURL)
}

func parseTokenEnv(content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimPrefix(line, "export ")
		if !strings.HasPrefix(line, "NIO_TOD_SSO_TOKEN=") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, "NIO_TOD_SSO_TOKEN="))
		value = strings.Trim(value, `"'`)
		if value != "" {
			return value
		}
	}
	return ""
}

func (m *todSSOManager) cachedToken(userID string) string {
	cred, ok := m.loadCredential(userID)
	if !ok || strings.TrimSpace(cred.Token) == "" {
		return ""
	}
	if m.credentialExpired(cred) {
		return ""
	}
	return cred.Token
}

// loadCredential returns the cached credential for userID. The cache file is
// re-read whenever its mtime changes, so an operator can edit the token in
// place and have the next turn pick it up without restarting the daemon.
func (m *todSSOManager) loadCredential(userID string) (todSSOCredential, bool) {
	path := m.cachePath(userID)
	info, statErr := os.Stat(path)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cache == nil {
		m.cache = make(map[string]todSSOCredential)
	}
	if m.cacheModTime == nil {
		m.cacheModTime = make(map[string]time.Time)
	}
	if cred, ok := m.cache[userID]; ok {
		if statErr == nil && !info.ModTime().After(m.cacheModTime[userID]) {
			return cred, strings.TrimSpace(cred.Token) != ""
		}
		// The file was deleted or edited: fall through and re-read it. A
		// deleted file therefore forces a fresh login.
		delete(m.cache, userID)
		delete(m.cacheModTime, userID)
	}

	cred, ok := m.readCredentialFile(path)
	if !ok {
		// Cache files written before the readable-name layout are keyed by a
		// hash of the user ID. Keep honoring them to avoid a forced re-login
		// for every user after an upgrade.
		cred, ok = m.readCredentialFile(m.legacyCachePath(userID))
	}
	if !ok {
		return todSSOCredential{}, false
	}
	m.cache[userID] = cred
	if statErr == nil {
		m.cacheModTime[userID] = info.ModTime()
	}
	return cred, strings.TrimSpace(cred.Token) != ""
}

func (m *todSSOManager) readCredentialFile(path string) (todSSOCredential, bool) {
	if strings.TrimSpace(path) == "" {
		return todSSOCredential{}, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return todSSOCredential{}, false
	}
	var cred todSSOCredential
	if err := json.Unmarshal(data, &cred); err != nil {
		return todSSOCredential{}, false
	}
	if strings.TrimSpace(cred.Token) == "" {
		return todSSOCredential{}, false
	}
	return cred, true
}

// credentialExpired prefers tod-sso's expires_at and falls back to the JWT
// exp claim. Opaque or malformed tokens are treated as usable; tod-sso remains
// the authority.
func (m *todSSOManager) credentialExpired(cred todSSOCredential) bool {
	if expiresAt := strings.TrimSpace(cred.ExpiresAt); expiresAt != "" {
		if parsed, err := time.Parse(time.RFC3339, expiresAt); err == nil {
			now := time.Now
			if m.now != nil {
				now = m.now
			}
			// Refresh a little early so a token cannot expire mid-turn.
			return now().Unix() >= parsed.Unix()-60
		}
	}
	return m.tokenExpired(cred.Token)
}

// tokenExpired decodes the JWT exp claim when the credential is a JWT. Opaque
// or malformed tokens are treated as usable; tod-sso remains the authority.
func (m *todSSOManager) tokenExpired(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return false
	}
	now := time.Now
	if m.now != nil {
		now = m.now
	}
	// Refresh a little early so a token cannot expire mid-turn.
	return now().Unix() >= claims.Exp-60
}

func (m *todSSOManager) storeToken(userID, token string) {
	m.storeCredential(todSSOCredential{UserID: userID, Token: token})
}

// storeCredential persists one user's access and refresh tokens in a dedicated
// 0600 JSON file so operators can inspect, edit, or delete it directly.
func (m *todSSOManager) storeCredential(cred todSSOCredential) {
	cred.UserID = strings.TrimSpace(cred.UserID)
	cred.Token = strings.TrimSpace(cred.Token)
	if cred.UserID == "" || cred.Token == "" {
		return
	}
	now := time.Now
	if m.now != nil {
		now = m.now
	}
	if cred.UpdatedAt.IsZero() {
		cred.UpdatedAt = now().UTC()
	}

	m.mu.Lock()
	if m.cache == nil {
		m.cache = make(map[string]todSSOCredential)
	}
	if m.cacheModTime == nil {
		m.cacheModTime = make(map[string]time.Time)
	}
	m.cache[cred.UserID] = cred
	m.mu.Unlock()

	if err := os.MkdirAll(m.cfg.cacheDir, 0o700); err != nil {
		slog.Warn("codex: cannot create TOD SSO cache directory",
			"dir", m.cfg.cacheDir, "error", err)
		return
	}
	_ = os.Chmod(m.cfg.cacheDir, 0o700)
	payload, err := json.MarshalIndent(cred, "", "  ")
	if err != nil {
		slog.Warn("codex: cannot encode TOD SSO cache entry", "error", err)
		return
	}
	payload = append(payload, '\n')
	path := m.cachePath(cred.UserID)
	tmp, err := os.CreateTemp(m.cfg.cacheDir, ".cred-*.tmp")
	if err != nil {
		slog.Warn("codex: cannot write TOD SSO cache entry", "error", err)
		return
	}
	tmpPath := tmp.Name()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		slog.Warn("codex: cannot secure TOD SSO cache entry", "error", err)
		return
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		slog.Warn("codex: cannot write TOD SSO cache entry", "error", err)
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		slog.Warn("codex: cannot close TOD SSO cache entry", "error", err)
		return
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		slog.Warn("codex: cannot replace TOD SSO cache entry", "error", err)
		return
	}
	if info, err := os.Stat(path); err == nil {
		m.mu.Lock()
		m.cacheModTime[cred.UserID] = info.ModTime()
		m.mu.Unlock()
	}
}

// cachePath names one file per user after the platform user ID so the cache is
// easy to find and edit. IDs containing unsafe path characters get a short
// hash suffix to keep distinct users in distinct files.
func (m *todSSOManager) cachePath(userID string) string {
	trimmed := strings.TrimSpace(userID)
	name := sanitizeCacheName(trimmed)
	if name == "" {
		name = "user"
	}
	if name != trimmed {
		sum := sha256.Sum256([]byte(userID))
		name += "-" + hex.EncodeToString(sum[:4])
	}
	if len(name) > 96 {
		name = name[:96]
	}
	return filepath.Join(m.cfg.cacheDir, name+".json")
}

func (m *todSSOManager) legacyCachePath(userID string) string {
	sum := sha256.Sum256([]byte(userID))
	return filepath.Join(m.cfg.cacheDir, hex.EncodeToString(sum[:16])+".json")
}

func sanitizeCacheName(userID string) string {
	var b strings.Builder
	for _, r := range userID {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

type tailBuffer struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func (b *tailBuffer) WriteString(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, s...)
	if b.limit > 0 && len(b.buf) > b.limit {
		b.buf = b.buf[len(b.buf)-b.limit:]
	}
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
