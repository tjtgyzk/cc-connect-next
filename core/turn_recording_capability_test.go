package core

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestTurnRecordingCapabilityReportsWriteContract(t *testing.T) {
	p := newTurnRecordPlatform()
	e := NewEngine("project", &stubAgent{}, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	e.OnPlatformReady(p)
	for _, enabled := range []bool{false, true} {
		p.enabled = enabled
		manifest := e.QueryAgentCapabilityManifest("", "", false)
		found := false
		for _, adapter := range manifest.Runtime {
			for _, feature := range adapter.Capabilities {
				if feature.ID != "turn_recording" {
					continue
				}
				found = true
				wantState := CapabilityUnavailable
				if enabled {
					wantState = CapabilityConditional
				}
				if feature.Availability.State != wantState || feature.Permission != CapabilityPermissionConditional || feature.ReadOnly == nil || *feature.ReadOnly || len(feature.SideEffects) == 0 {
					t.Fatalf("enabled=%v: incomplete write contract: %+v", enabled, feature)
				}
				data, err := json.Marshal(feature)
				if err != nil {
					t.Fatal(err)
				}
				var wire map[string]any
				if err := json.Unmarshal(data, &wire); err != nil {
					t.Fatal(err)
				}
				if readOnly, ok := wire["read_only"].(bool); !ok || readOnly {
					t.Fatalf("write side effect lost during serialization: %s", data)
				}
			}
		}
		if !found {
			t.Fatal("turn_recording capability missing")
		}
	}
}
