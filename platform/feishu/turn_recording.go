package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
	larkwiki "github.com/larksuite/oapi-sdk-go/v3/service/wiki/v2"
	"github.com/timmyagentic/cc-connect-next/core"
)

const (
	// Default target: the NIO wiki-hosted Bitable shared by the requester.
	defaultTurnRecordWikiToken = "F9ETwBtfRilKbfkMc03cEnvJn2f"
	defaultTurnRecordTableID   = "tblPMqd52KLMKqrh"

	turnRecordWriteTimeout = 15 * time.Second
	turnRecordQueueSize    = 256
)

// turnRecordFields maps logical columns to the actual Bitable column names.
type turnRecordFields struct {
	Time         string
	Name         string
	UserID       string
	Chat         string
	Model        string
	Question     string
	Answer       string
	Requester    string
	Trace        string
	Duration     string
	ToolCount    string
	Summary      string
	TaskID       string
	TurnCount    string
	Conversation string
	TraceHistory string
	ArchiveState string
	RecordKind   string
}

func defaultTurnRecordFields() turnRecordFields {
	return turnRecordFields{
		Time:     "时间",
		Name:     "昵称",
		UserID:   "user_id",
		Chat:     "群名",
		Model:    "模型",
		Question: "问题",
		Answer:   "答案",
	}
}

type turnRecordConfig struct {
	enabled           bool
	mode              string
	wikiToken         string
	appToken          string
	tableID           string
	fields            turnRecordFields
	traceSeedTemplate string
	traceURLPrefix    string
}

// parseTurnRecordOptions validates and builds the turn-recording config.
func parseTurnRecordOptions(opts map[string]any) (*turnRecordConfig, error) {
	cfg := &turnRecordConfig{
		mode:      turnRecordModeTurn,
		wikiToken: defaultTurnRecordWikiToken,
		tableID:   defaultTurnRecordTableID,
		fields:    defaultTurnRecordFields(),
	}
	if raw, ok := opts["turn_record_mode"]; ok {
		mode, ok := raw.(string)
		if !ok || (mode != turnRecordModeTurn && mode != turnRecordModeTopic) {
			return nil, fmt.Errorf("turn_record_mode must be turn or topic")
		}
		cfg.mode = mode
	}
	if raw, ok := opts["turn_record_enabled"]; ok {
		v, ok := raw.(bool)
		if !ok {
			return nil, fmt.Errorf("turn_record_enabled must be a boolean")
		}
		cfg.enabled = v
	}
	if v, ok := opts["turn_record_wiki_token"].(string); ok && strings.TrimSpace(v) != "" {
		cfg.wikiToken = strings.TrimSpace(v)
	}
	if v, ok := opts["turn_record_app_token"].(string); ok && strings.TrimSpace(v) != "" {
		cfg.appToken = strings.TrimSpace(v)
	}
	if v, ok := opts["turn_record_table_id"].(string); ok && strings.TrimSpace(v) != "" {
		cfg.tableID = strings.TrimSpace(v)
	}
	if err := cfg.parseTraceOptions(opts); err != nil {
		return nil, err
	}
	if raw, ok := opts["turn_record_fields"]; ok {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("turn_record_fields must be a table")
		}
		set := func(target *string, key string) {
			if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
				*target = strings.TrimSpace(v)
			}
		}
		for _, key := range []string{"time", "name", "user_id", "chat", "model", "question", "answer"} {
			if value, exists := m[key]; exists {
				if _, ok := value.(string); !ok {
					return nil, fmt.Errorf("turn_record_fields.%s must be a string", key)
				}
			}
		}
		set(&cfg.fields.Time, "time")
		set(&cfg.fields.Name, "name")
		set(&cfg.fields.UserID, "user_id")
		set(&cfg.fields.Chat, "chat")
		set(&cfg.fields.Model, "model")
		set(&cfg.fields.Question, "question")
		set(&cfg.fields.Answer, "answer")
		optional := map[string]*string{
			"requester": &cfg.fields.Requester, "trace": &cfg.fields.Trace,
			"duration": &cfg.fields.Duration, "tool_count": &cfg.fields.ToolCount,
			"summary": &cfg.fields.Summary,
			"task_id": &cfg.fields.TaskID, "turn_count": &cfg.fields.TurnCount,
			"conversation": &cfg.fields.Conversation, "trace_history": &cfg.fields.TraceHistory,
			"archive_state": &cfg.fields.ArchiveState, "record_kind": &cfg.fields.RecordKind,
		}
		used := map[string]bool{}
		for _, column := range []string{cfg.fields.Time, cfg.fields.Name, cfg.fields.UserID, cfg.fields.Chat, cfg.fields.Model, cfg.fields.Question, cfg.fields.Answer} {
			if used[column] {
				return nil, fmt.Errorf("turn_record_fields must use distinct columns")
			}
			used[column] = true
		}
		for _, key := range []string{"requester", "trace", "duration", "tool_count", "summary", "task_id", "turn_count", "conversation", "trace_history", "archive_state", "record_kind"} {
			if raw, ok := m[key]; ok {
				column, ok := raw.(string)
				if !ok {
					return nil, fmt.Errorf("turn_record_fields.%s must be a string", key)
				}
				column = strings.TrimSpace(column)
				if column == "" {
					continue
				}
				if used[column] {
					return nil, fmt.Errorf("turn_record_fields.%s must use a distinct column", key)
				}
				used[column] = true
				*optional[key] = column
			}
		}
	}
	if cfg.mode == turnRecordModeTopic {
		for key, value := range map[string]string{"task_id": cfg.fields.TaskID, "turn_count": cfg.fields.TurnCount, "conversation": cfg.fields.Conversation, "trace_history": cfg.fields.TraceHistory, "archive_state": cfg.fields.ArchiveState, "record_kind": cfg.fields.RecordKind} {
			if value == "" {
				return nil, fmt.Errorf("turn_record_mode topic requires turn_record_fields.%s", key)
			}
		}
	}
	if cfg.enabled {
		if cfg.appToken == "" && cfg.wikiToken == "" {
			return nil, fmt.Errorf("turn_record requires turn_record_app_token or turn_record_wiki_token")
		}
		if cfg.tableID == "" {
			return nil, fmt.Errorf("turn_record requires turn_record_table_id")
		}
	}
	return cfg, nil
}

// turnRecorder owns the async write queue so recording never blocks a turn.
type turnRecorder struct {
	cfg    *turnRecordConfig
	queue  chan core.TurnRecord
	ctx    context.Context
	cancel context.CancelFunc

	appTokenMu sync.Mutex
	appToken   string
	writeMu    sync.Mutex // one read/merge/write at a time within this recorder; not a cross-process CAS
}

func newTurnRecorder(cfg *turnRecordConfig) *turnRecorder {
	ctx, cancel := context.WithCancel(context.Background())
	return &turnRecorder{
		cfg:    cfg,
		queue:  make(chan core.TurnRecord, turnRecordQueueSize),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (r *turnRecorder) run(p *Platform) {
	for {
		select {
		case <-r.ctx.Done():
			return
		case rec := <-r.queue:
			r.write(p, rec)
		}
	}
}

func (r *turnRecorder) stop() {
	if r == nil {
		return
	}
	r.cancel()
}

// enqueue is non-blocking: a full queue drops the record rather than delaying
// the turn. Recording is best-effort audit, never a delivery guarantee.
func (r *turnRecorder) enqueue(rec core.TurnRecord) error {
	select {
	case r.queue <- rec:
		return nil
	default:
		return fmt.Errorf("turn record queue full (%d)", cap(r.queue))
	}
}

func (r *turnRecorder) resolveAppToken(ctx context.Context, p *Platform) (string, error) {
	r.appTokenMu.Lock()
	cached := r.appToken
	r.appTokenMu.Unlock()
	if cached != "" {
		return cached, nil
	}
	if r.cfg.appToken != "" {
		r.appTokenMu.Lock()
		r.appToken = r.cfg.appToken
		r.appTokenMu.Unlock()
		return r.cfg.appToken, nil
	}

	var resp *larkwiki.GetNodeSpaceResp
	err := p.withFreshTenantAccessTokenRetry(ctx, "resolve wiki node", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
		var callErr error
		resp, callErr = client.Wiki.Space.GetNode(ctx,
			larkwiki.NewGetNodeSpaceReqBuilder().Token(r.cfg.wikiToken).ObjType("wiki").Build(),
			options...)
		if callErr != nil {
			return fmt.Errorf("%s: resolve wiki node: %w", p.tag(), callErr)
		}
		if !resp.Success() {
			return fmt.Errorf("%s: resolve wiki node failed code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if resp == nil || resp.Data == nil || resp.Data.Node == nil || resp.Data.Node.ObjToken == nil {
		return "", fmt.Errorf("%s: wiki node %s has no obj_token", p.tag(), r.cfg.wikiToken)
	}
	token := *resp.Data.Node.ObjToken
	r.appTokenMu.Lock()
	r.appToken = token
	r.appTokenMu.Unlock()
	return token, nil
}

func (r *turnRecorder) write(p *Platform, rec core.TurnRecord) {
	ctx, cancel := context.WithTimeout(r.ctx, turnRecordWriteTimeout)
	defer cancel()

	appToken, err := r.resolveAppToken(ctx, p)
	if err != nil {
		slog.Warn(p.tag()+": turn record resolve app token failed", "error", err)
		return
	}

	userID, name := p.resolveUserInfo(ctx, rec.UserID)
	name = turnRecordDisplayName(name, rec.UserID, userID, rec.FeishuUserID)
	if name == "" {
		name = turnRecordDisplayName(rec.UserName, rec.UserID, userID, rec.FeishuUserID)
	}
	if userID == "" {
		userID = rec.FeishuUserID
	}
	if r.cfg.mode == turnRecordModeTopic {
		if err := r.writeTopic(ctx, p, appToken, topicArchiveTurn{Record: rec, DisplayName: name, TenantUserID: userID}); err != nil {
			slog.Warn(p.tag()+": topic turn record write failed", "error", err, "message_id", rec.MessageID)
		}
		return
	}

	f := r.cfg.fields
	fields := map[string]any{
		f.Time:     rec.At.Format("2006-01-02 15:04:05"),
		f.Name:     name,
		f.UserID:   userID,
		f.Chat:     rec.ChatName,
		f.Model:    rec.Model,
		f.Question: rec.Question,
		f.Answer:   rec.Answer,
	}
	if f.Requester != "" && strings.HasPrefix(rec.UserID, "ou_") && len(rec.UserID) > len("ou_") && isValidFeishuLookupID(rec.UserID) {
		fields[f.Requester] = []map[string]string{{"id": rec.UserID}}
	}
	if f.Trace != "" {
		traceURL := rec.TraceURL
		if traceURL == "" {
			traceURL = r.cfg.traceURL(rec.AgentSessionID, rec.AgentTurnID)
		}
		if traceURL != "" {
			fields[f.Trace] = map[string]string{"text": "Trace", "link": traceURL}
		}
	}
	if f.Duration != "" && rec.Duration > 0 {
		fields[f.Duration] = rec.Duration.Seconds()
	}
	if f.ToolCount != "" && rec.ToolCount >= 0 {
		fields[f.ToolCount] = rec.ToolCount
	}
	if f.Summary != "" && rec.Summary != "" {
		fields[f.Summary] = rec.Summary
	}

	req := larkbitable.NewCreateAppTableRecordReqBuilder().
		AppToken(appToken).
		TableId(r.cfg.tableID).
		UserIdType("open_id").
		ClientToken(turnRecordClientToken(rec)).
		AppTableRecord(larkbitable.NewAppTableRecordBuilder().Fields(fields).Build()).
		Build()

	err = p.withTransientRetry(ctx, "turn record", func() error {
		return p.withFreshTenantAccessTokenRetry(ctx, "turn record", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			resp, callErr := client.Bitable.AppTableRecord.Create(ctx, req, options...)
			if callErr != nil {
				return fmt.Errorf("%s: create bitable record: %w", p.tag(), callErr)
			}
			if !resp.Success() {
				return fmt.Errorf("%s: create bitable record failed code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
			}
			return nil
		})
	})
	if err != nil {
		slog.Warn(p.tag()+": turn record write failed", "error", err, "message_id", rec.MessageID)
	}
}

// Identity lookup failures may leave an ID in UserName. Keep that ID in its
// identity/person field instead of presenting it as a human display name.
func turnRecordDisplayName(name string, ids ...string) string {
	name = strings.TrimSpace(name)
	if name == "" || (isValidFeishuLookupID(name) && (strings.HasPrefix(name, "ou_") || strings.HasPrefix(name, "on_"))) {
		return ""
	}
	for _, id := range ids {
		if id != "" && name == strings.TrimSpace(id) {
			return ""
		}
	}
	return name
}

// turnRecordClientToken is a stable idempotency key so Feishu de-duplicates
// retried/redelivered writes for the same turn. Feishu requires the
// client_token query parameter to be a valid UUID v4: the version nibble must
// be 4 and the variant nibble must be 8/9/a/b. We derive 16 bytes from a
// SHA-256 hash, then set the version and variant bits so the result passes
// Feishu's UUID validation while remaining deterministic.
func turnRecordClientToken(rec core.TurnRecord) string {
	sum := sha256.Sum256([]byte(rec.MessageID + "\x00" + rec.Answer))
	b := sum[:16]
	// RFC 4122: set version (4) in bits 12-15 of time_hi_and_version.
	b[6] = (b[6] & 0x0f) | 0x40
	// RFC 4122: set variant to IETF (10xx) in bits 16-17 of clock_seq.
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

func (p *Platform) configureTurnRecording(opts map[string]any) error {
	cfg, err := parseTurnRecordOptions(opts)
	if err != nil {
		return err
	}
	if !cfg.enabled {
		return nil
	}
	recorder := newTurnRecorder(cfg)
	p.turnRecording = recorder
	go recorder.run(p)
	slog.Info(p.tag()+": turn recording enabled", "table_id", cfg.tableID, "wiki", cfg.wikiToken != "", "app_token", cfg.appToken != "")
	return nil
}

func (p *Platform) stopTurnRecording() {
	if p.turnRecording != nil {
		p.turnRecording.stop()
	}
}

// TurnRecordingEnabled reports whether turn recording is configured.
func (p *Platform) TurnRecordingEnabled() bool {
	return p.turnRecording != nil
}

// RecordTurn enqueues a delivered turn for asynchronous archival.
func (p *Platform) RecordTurn(_ context.Context, rec core.TurnRecord) error {
	if p.turnRecording == nil {
		return nil
	}
	return p.turnRecording.enqueue(rec)
}
