package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
	"github.com/timmyagentic/cc-connect-next/core"
)

const (
	turnRecordModeTurn   = "turn"
	turnRecordModeTopic  = "topic"
	topicArchiveVersion  = 1
	topicArchiveMainKind = "主记录"
	// Conservative byte bound: never truncate the canonical journal or silently
	// discard earlier answers when a Bitable text column fills up.
	topicArchiveTextLimit = 100000
)

type topicArchiveTurn struct {
	Key          string          `json:"key"`
	Record       core.TurnRecord `json:"record"`
	DisplayName  string          `json:"display_name"`
	TenantUserID string          `json:"tenant_user_id"`
}

type topicArchiveState struct {
	Version int                `json:"version"`
	TaskID  string             `json:"task_id"`
	Turns   []topicArchiveTurn `json:"turns"`
}

func topicArchiveHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func topicArchiveTurnKey(rec core.TurnRecord) (string, error) {
	if rec.AgentSessionID != "" && rec.AgentTurnID != "" {
		return "native:" + topicArchiveHash(rec.AgentSessionID+"\x00"+rec.AgentTurnID), nil
	}
	if rec.MessageID != "" {
		return "message:" + topicArchiveHash(rec.MessageID), nil
	}
	return "", fmt.Errorf("feishu: topic archive requires native turn identity or message ID")
}

func topicArchiveTaskID(rec core.TurnRecord) (string, error) {
	if isThreadSessionKey(rec.SessionKey) {
		if rec.ConversationID == "" {
			return "", fmt.Errorf("feishu: topic archive missing stable conversation ID")
		}
		return rec.ConversationID, nil
	}
	key, err := topicArchiveTurnKey(rec)
	if err != nil {
		return "", err
	}
	// A normal chat can contain unrelated tasks for months. Only actual topic
	// keys aggregate; normal messages get one independently recoverable record.
	return "turn:" + topicArchiveHash(rec.ConversationID+"\x00"+rec.SessionKey+"\x00"+key), nil
}

func validateTopicArchive(state topicArchiveState, taskID string) error {
	if state.Version != topicArchiveVersion || state.TaskID != taskID || len(state.Turns) == 0 {
		return fmt.Errorf("feishu: topic archive state version, task identity or turns invalid")
	}
	seen := make(map[string]bool)
	for _, turn := range state.Turns {
		key, err := topicArchiveTurnKey(turn.Record)
		if err != nil || key != turn.Key || seen[key] || turn.Record.At.IsZero() || turn.Record.Duration < 0 || turn.Record.ToolCount < 0 {
			return fmt.Errorf("feishu: topic archive contains invalid or duplicate turn state")
		}
		id, err := topicArchiveTaskID(turn.Record)
		if err != nil || id != taskID {
			return fmt.Errorf("feishu: topic archive contains a different task")
		}
		seen[key] = true
	}
	return nil
}

func decodeTopicArchive(raw, taskID string) (topicArchiveState, error) {
	var state topicArchiveState
	if raw == "" || len(raw) > topicArchiveTextLimit {
		return state, fmt.Errorf("feishu: topic archive state missing or oversized")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return state, fmt.Errorf("feishu: topic archive state is malformed")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return state, fmt.Errorf("feishu: topic archive state has trailing data")
	}
	return state, validateTopicArchive(state, taskID)
}

// Search returns text columns as either strings or ordered rich-text segments.
// Refuse other shapes rather than coercing a malformed state into an empty row.
func topicArchiveText(value any) (string, error) {
	if text, ok := value.(string); ok {
		return text, nil
	}
	segments, ok := value.([]any)
	if !ok {
		return "", fmt.Errorf("feishu: topic archive text has an invalid type")
	}
	var text strings.Builder
	for _, segment := range segments {
		part, ok := segment.(map[string]any)
		if !ok {
			return "", fmt.Errorf("feishu: topic archive text segment malformed")
		}
		kind, _ := part["type"].(string)
		value, ok := part["text"].(string)
		if !ok || (kind != "" && kind != "text") {
			return "", fmt.Errorf("feishu: topic archive text segment is not plain text")
		}
		text.WriteString(value)
	}
	return text.String(), nil
}

func (r *turnRecorder) topicArchiveFieldTypes() map[string]int {
	f := r.cfg.fields
	types := make(map[string]int)
	for _, name := range []string{f.Time, f.Name, f.UserID, f.Chat, f.Model, f.Question, f.Answer, f.Summary, f.TaskID, f.Conversation, f.TraceHistory, f.ArchiveState, f.RecordKind} {
		if name != "" {
			types[name] = 1
		}
	}
	for _, name := range []string{f.Duration, f.ToolCount, f.TurnCount} {
		if name != "" {
			types[name] = 2
		}
	}
	if f.Requester != "" {
		types[f.Requester] = 11
	}
	if f.Trace != "" {
		types[f.Trace] = 15
	}
	return types
}

func (r *turnRecorder) validateTopicArchiveFields(ctx context.Context, p *Platform, appToken string) error {
	want := r.topicArchiveFieldTypes()
	pageToken := ""
	seenPages := map[string]bool{}
	for {
		var response *larkbitable.ListAppTableFieldResp
		err := p.withFreshTenantAccessTokenRetry(ctx, "topic archive fields", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
			var err error
			response, err = client.Bitable.AppTableField.List(ctx, larkbitable.NewListAppTableFieldReqBuilder().AppToken(appToken).TableId(r.cfg.tableID).PageSize(100).PageToken(pageToken).Build(), options...)
			if err != nil {
				return fmt.Errorf("feishu: read topic archive fields: %w", err)
			}
			if response != nil && !response.Success() {
				return fmt.Errorf("feishu: read topic archive fields failed code=%d", response.Code)
			}
			if response == nil || response.Data == nil {
				return fmt.Errorf("feishu: read topic archive fields failed")
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, field := range response.Data.Items {
			if field == nil || field.FieldName == nil {
				continue
			}
			if kind, ok := want[*field.FieldName]; ok {
				if field.Type == nil || *field.Type != kind {
					return fmt.Errorf("feishu: topic archive column %q must have type %d", *field.FieldName, kind)
				}
				delete(want, *field.FieldName)
			}
		}
		if response.Data.HasMore == nil || !*response.Data.HasMore {
			break
		}
		pageToken = stringValue(response.Data.PageToken)
		if pageToken == "" || seenPages[pageToken] {
			return fmt.Errorf("feishu: topic archive fields pagination invalid")
		}
		seenPages[pageToken] = true
	}
	if len(want) > 0 {
		return fmt.Errorf("feishu: topic archive is missing %d configured columns", len(want))
	}
	return nil
}

func (r *turnRecorder) readTopicArchive(ctx context.Context, p *Platform, appToken, taskID string) (string, topicArchiveState, error) {
	state := topicArchiveState{Version: topicArchiveVersion, TaskID: taskID}
	body := larkbitable.NewSearchAppTableRecordReqBodyBuilder().
		FieldNames([]string{r.cfg.fields.TaskID, r.cfg.fields.ArchiveState}).
		Filter(larkbitable.NewFilterInfoBuilder().Conjunction("and").Conditions([]*larkbitable.Condition{
			larkbitable.NewConditionBuilder().FieldName(r.cfg.fields.TaskID).Operator("is").Value([]string{taskID}).Build(),
		}).Build()).Build()
	var response *larkbitable.SearchAppTableRecordResp
	err := p.withFreshTenantAccessTokenRetry(ctx, "search topic archive", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
		var err error
		response, err = client.Bitable.AppTableRecord.Search(ctx, larkbitable.NewSearchAppTableRecordReqBuilder().AppToken(appToken).TableId(r.cfg.tableID).UserIdType("open_id").PageSize(2).Body(body).Build(), options...)
		if err != nil {
			return fmt.Errorf("feishu: search topic archive: %w", err)
		}
		if response != nil && !response.Success() {
			return fmt.Errorf("feishu: search topic archive failed code=%d", response.Code)
		}
		if response == nil || response.Data == nil {
			return fmt.Errorf("feishu: search topic archive failed")
		}
		return nil
	})
	if err != nil {
		return "", state, err
	}
	if len(response.Data.Items) > 1 || (response.Data.HasMore != nil && *response.Data.HasMore) || (response.Data.Total != nil && *response.Data.Total > 1) {
		return "", state, fmt.Errorf("feishu: duplicate task IDs in topic archive; refusing overwrite")
	}
	if len(response.Data.Items) == 0 {
		return "", state, nil
	}
	row := response.Data.Items[0]
	if row == nil || row.RecordId == nil || *row.RecordId == "" {
		return "", state, fmt.Errorf("feishu: topic archive record ID missing")
	}
	id, err := topicArchiveText(row.Fields[r.cfg.fields.TaskID])
	if err != nil || id != taskID {
		return "", state, fmt.Errorf("feishu: topic archive search returned a different task")
	}
	raw, err := topicArchiveText(row.Fields[r.cfg.fields.ArchiveState])
	if err != nil {
		return "", state, err
	}
	state, err = decodeTopicArchive(raw, taskID)
	return *row.RecordId, state, err
}

func mergeTopicArchive(state topicArchiveState, next topicArchiveTurn) (topicArchiveState, bool, error) {
	for _, previous := range state.Turns {
		if previous.Key != next.Key {
			continue
		}
		oldRecord, newRecord := previous.Record, next.Record
		oldRecord.At, newRecord.At = oldRecord.At.UTC(), newRecord.At.UTC()
		if !reflect.DeepEqual(oldRecord, newRecord) {
			return state, false, fmt.Errorf("feishu: same archived turn has conflicting content; refusing overwrite")
		}
		return state, false, nil
	}
	state.Turns = append(state.Turns, next)
	sort.SliceStable(state.Turns, func(i, j int) bool {
		if state.Turns[i].Record.At.Equal(state.Turns[j].Record.At) {
			return state.Turns[i].Key < state.Turns[j].Key
		}
		return state.Turns[i].Record.At.Before(state.Turns[j].Record.At)
	})
	return state, true, validateTopicArchive(state, state.TaskID)
}

func (r *turnRecorder) topicArchiveFields(state topicArchiveState) (map[string]any, error) {
	if err := validateTopicArchive(state, state.TaskID); err != nil {
		return nil, err
	}
	first, last := state.Turns[0], state.Turns[len(state.Turns)-1]
	f := r.cfg.fields
	fields := map[string]any{
		f.Time: last.Record.At.Format("2006-01-02 15:04:05"), f.Name: first.DisplayName,
		f.UserID: first.TenantUserID, f.Chat: last.Record.ChatName, f.Model: last.Record.Model,
		f.Question: first.Record.Question, f.Answer: last.Record.Answer,
		f.TaskID: state.TaskID, f.TurnCount: len(state.Turns), f.RecordKind: topicArchiveMainKind,
	}
	if f.Summary != "" {
		fields[f.Summary] = first.Record.Summary
	}
	if f.Requester != "" && strings.HasPrefix(first.Record.UserID, "ou_") && isValidFeishuLookupID(first.Record.UserID) {
		fields[f.Requester] = []map[string]string{{"id": first.Record.UserID}}
	}
	if f.Trace != "" {
		fields[f.Trace] = nil
		if last.Record.TraceURL != "" {
			fields[f.Trace] = map[string]string{"text": "Trace", "link": last.Record.TraceURL}
		}
	}
	var conversation, traces strings.Builder
	var duration float64
	var tools int
	language := first.Record.Language
	if language == core.LangAuto {
		language = core.LangChinese
	}
	i18n := core.NewI18n(language)
	for index, turn := range state.Turns {
		if index > 0 {
			conversation.WriteString("\n\n---\n\n")
		}
		name := turn.DisplayName
		if name == "" {
			name = turn.Record.UserID
		}
		turnLabel := i18n.Tf(core.MsgTopicArchiveTurn, index+1)
		fmt.Fprintf(&conversation, "%s · %s · %s\n%s\n%s\n\n%s\n%s", turnLabel, turn.Record.At.Format("2006-01-02 15:04:05"), name, i18n.T(core.MsgTopicArchiveQuestion), turn.Record.Question, i18n.T(core.MsgTopicArchiveAnswer), turn.Record.Answer)
		if turn.Record.TraceURL != "" {
			fmt.Fprintf(&traces, "%s · %s\n", turnLabel, turn.Record.TraceURL)
		}
		duration += turn.Record.Duration.Seconds()
		if turn.Record.ToolCount > math.MaxInt-tools {
			return nil, fmt.Errorf("feishu: topic archive tool count overflow")
		}
		tools += turn.Record.ToolCount
	}
	fields[f.Conversation] = conversation.String()
	fields[f.TraceHistory] = strings.TrimSpace(traces.String())
	if f.Duration != "" {
		fields[f.Duration] = nil
		if duration > 0 {
			fields[f.Duration] = duration
		}
	}
	if f.ToolCount != "" {
		fields[f.ToolCount] = tools
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("feishu: encode topic archive state: %w", err)
	}
	fields[f.ArchiveState] = string(encoded)
	for _, value := range fields {
		if text, ok := value.(string); ok && len(text) > topicArchiveTextLimit {
			return nil, fmt.Errorf("feishu: topic archive exceeds text capacity; refusing truncation")
		}
	}
	return fields, nil
}

// writeTopic is a single-instance read/merge/upsert. Bitable does not supply a
// compare-and-swap here: deployments must have one archive writer per table.
// Every retry rereads canonical state, including after an ambiguous write.
func (r *turnRecorder) writeTopic(ctx context.Context, p *Platform, appToken string, next topicArchiveTurn) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	var err error
	next.Key, err = topicArchiveTurnKey(next.Record)
	if err != nil {
		return err
	}
	taskID, err := topicArchiveTaskID(next.Record)
	if err != nil {
		return err
	}
	if next.Record.TraceURL == "" {
		next.Record.TraceURL = r.cfg.traceURL(next.Record.AgentSessionID, next.Record.AgentTurnID)
	}
	// Strip monotonic clock data so a retry matches JSON recovered after restart.
	next.Record.At = next.Record.At.Round(0)
	if err := r.validateTopicArchiveFields(ctx, p, appToken); err != nil {
		return err
	}
	var writeErr error
	for attempt := 0; attempt < 4; attempt++ {
		id, state, err := r.readTopicArchive(ctx, p, appToken, taskID)
		if err != nil {
			return err
		}
		state, changed, err := mergeTopicArchive(state, next)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		// The final pass only checks whether the prior ambiguous write landed.
		if attempt == 3 {
			return fmt.Errorf("feishu: topic archive write not verified: %w", writeErr)
		}
		fields, err := r.topicArchiveFields(state)
		if err != nil {
			return err
		}
		writeErr = r.saveTopicArchive(ctx, p, appToken, id, taskID, fields)
		if writeErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("feishu: topic archive write outcome unknown; safe to retry: %w", ctx.Err())
		}
	}
	return writeErr
}

func (r *turnRecorder) saveTopicArchive(ctx context.Context, p *Platform, appToken, recordID, taskID string, fields map[string]any) error {
	return p.withFreshTenantAccessTokenRetry(ctx, "save topic archive", func(client *lark.Client, options ...larkcore.RequestOptionFunc) error {
		body := larkbitable.NewAppTableRecordBuilder().Fields(fields).Build()
		if recordID == "" {
			response, err := client.Bitable.AppTableRecord.Create(ctx, larkbitable.NewCreateAppTableRecordReqBuilder().AppToken(appToken).TableId(r.cfg.tableID).UserIdType("open_id").ClientToken(turnRecordClientToken(core.TurnRecord{MessageID: "topic:" + taskID})).AppTableRecord(body).Build(), options...)
			if err != nil {
				return fmt.Errorf("feishu: create topic archive: %w", err)
			}
			if response != nil && !response.Success() {
				return fmt.Errorf("feishu: create topic archive failed code=%d", response.Code)
			}
			if response == nil || response.Data == nil || response.Data.Record == nil {
				return fmt.Errorf("feishu: create topic archive returned no successful record")
			}
			return nil
		}
		response, err := client.Bitable.AppTableRecord.Update(ctx, larkbitable.NewUpdateAppTableRecordReqBuilder().AppToken(appToken).TableId(r.cfg.tableID).RecordId(recordID).UserIdType("open_id").AppTableRecord(body).Build(), options...)
		if err != nil {
			return fmt.Errorf("feishu: update topic archive: %w", err)
		}
		if response != nil && !response.Success() {
			return fmt.Errorf("feishu: update topic archive failed code=%d", response.Code)
		}
		if response == nil || response.Data == nil || response.Data.Record == nil {
			return fmt.Errorf("feishu: update topic archive returned no successful record")
		}
		return nil
	})
}
