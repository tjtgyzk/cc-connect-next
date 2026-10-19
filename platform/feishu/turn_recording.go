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
	Time     string
	Name     string
	UserID   string
	Chat     string
	Model    string
	Question string
	Answer   string
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
	enabled   bool
	wikiToken string
	appToken  string
	tableID   string
	fields    turnRecordFields
}

// parseTurnRecordOptions validates and builds the turn-recording config.
func parseTurnRecordOptions(opts map[string]any) (*turnRecordConfig, error) {
	cfg := &turnRecordConfig{
		wikiToken: defaultTurnRecordWikiToken,
		tableID:   defaultTurnRecordTableID,
		fields:    defaultTurnRecordFields(),
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
		set(&cfg.fields.Time, "time")
		set(&cfg.fields.Name, "name")
		set(&cfg.fields.UserID, "user_id")
		set(&cfg.fields.Chat, "chat")
		set(&cfg.fields.Model, "model")
		set(&cfg.fields.Question, "question")
		set(&cfg.fields.Answer, "answer")
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

	userID, name := p.resolveUserInfo(rec.UserID)
	if name == "" {
		name = rec.UserName
	}
	if userID == "" {
		userID = rec.FeishuUserID
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

	req := larkbitable.NewCreateAppTableRecordReqBuilder().
		AppToken(appToken).
		TableId(r.cfg.tableID).
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

// turnRecordClientToken is a stable idempotency key so Feishu de-duplicates
// retried/redelivered writes for the same turn.
func turnRecordClientToken(rec core.TurnRecord) string {
	sum := sha256.Sum256([]byte(rec.MessageID + "\x00" + rec.Answer))
	return hex.EncodeToString(sum[:16])
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
