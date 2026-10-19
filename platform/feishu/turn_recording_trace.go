package feishu

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

func (cfg *turnRecordConfig) parseTraceOptions(opts map[string]any) error {
	if raw, ok := opts["turn_record_trace_seed_template"]; ok {
		seed, ok := raw.(string)
		if !ok {
			return fmt.Errorf("turn_record_trace_seed_template must be a string")
		}
		if strings.TrimSpace(seed) != "" {
			if !strings.Contains(seed, "{session_id}") || !strings.Contains(seed, "{turn_id}") {
				return fmt.Errorf("turn_record_trace_seed_template must contain {session_id} and {turn_id}")
			}
			remainder := strings.NewReplacer("{session_id}", "", "{turn_id}", "").Replace(seed)
			if strings.ContainsAny(remainder, "{}") {
				return fmt.Errorf("turn_record_trace_seed_template contains an unknown or malformed placeholder")
			}
			// Hash the exact configured bytes, including literal whitespace.
			cfg.traceSeedTemplate = seed
		}
	}
	if raw, ok := opts["turn_record_trace_url_prefix"]; ok {
		prefix, ok := raw.(string)
		if !ok {
			return fmt.Errorf("turn_record_trace_url_prefix must be a string")
		}
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			u, err := url.Parse(prefix)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(prefix, "#") {
				return fmt.Errorf("turn_record_trace_url_prefix must be an HTTP(S) URL without user information, query or fragment")
			}
			cfg.traceURLPrefix = strings.TrimRight(prefix, "/") + "/"
		}
	}
	return nil
}

// traceURL reproduces an explicitly configured exporter trace-ID scheme. It
// never searches by time/session alone and does not assert successful upload.
func (cfg *turnRecordConfig) traceURL(sessionID, turnID string) string {
	if cfg.traceSeedTemplate == "" || cfg.traceURLPrefix == "" || strings.TrimSpace(sessionID) == "" || strings.TrimSpace(turnID) == "" {
		return ""
	}
	seed := strings.NewReplacer("{session_id}", sessionID, "{turn_id}", turnID).Replace(cfg.traceSeedTemplate)
	sum := sha256.Sum256([]byte(seed))
	return cfg.traceURLPrefix + hex.EncodeToString(sum[:16])
}
