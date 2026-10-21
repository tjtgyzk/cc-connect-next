package feishu

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func timingMetricMapping() map[string]any {
	return map[string]any{
		"active_duration": "净处理秒数", "user_wait_duration": "等待秒数",
		"timed_turn_count": "有效计时轮数", "slow_turn_count": "慢回复数",
		"mean_turn_duration": "平均单轮秒数", "slow_turn_ratio": "慢回复比例",
	}
}

func enableTimingMetrics(t *testing.T, recorder *turnRecorder, fixture *topicArchiveServer) {
	t.Helper()
	opts := topicArchiveOptions()
	for key, value := range timingMetricMapping() {
		opts["turn_record_fields"].(map[string]any)[key] = value
	}
	cfg, err := parseTurnRecordOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	recorder.cfg = cfg
	fixture.types = recorder.topicArchiveFieldTypes()
}

func timedArchiveExample(index int, duration, wait time.Duration, timing string) topicArchiveTurn {
	turn := topicArchiveExample(index)
	turn.Record.Duration, turn.Record.UserWaitDuration, turn.Record.UserWaitTiming = duration, wait, timing
	turn.Key, _ = topicArchiveTurnKey(turn.Record)
	return turn
}

func assertTimingFields(t *testing.T, fields map[string]any, expected map[string]any) {
	t.Helper()
	for key, want := range expected {
		got, exists := fields[key]
		if !exists {
			t.Fatalf("missing metric %q", key)
		}
		if want == nil {
			if got != nil {
				t.Fatalf("%s = %v, want null", key, got)
			}
			continue
		}
		var number float64
		switch value := got.(type) {
		case int:
			number = float64(value)
		case float64:
			number = value
		default:
			t.Fatalf("%s is not a number: %T", key, got)
		}
		if math.Abs(number-want.(float64)) > 1e-9 {
			t.Fatalf("%s = %v, want %v", key, got, want)
		}
	}
}

func TestTopicArchive_TimingSubtractsWaitAndCountsSlowTurnsNotCumulativeTopics(t *testing.T) {
	_, recorder, fixture := newTopicArchiveFixture(t)
	enableTimingMetrics(t, recorder, fixture)
	state := topicArchiveState{Version: 1, TaskID: "conversation_1", Turns: []topicArchiveTurn{
		timedArchiveExample(1, 350*time.Second, 100*time.Second, "measured"),
		timedArchiveExample(2, 301*time.Second, 0, "reconstructed"),
		timedArchiveExample(3, 300*time.Second, 0, "measured"),
		timedArchiveExample(4, 900*time.Second, 0, ""),
		timedArchiveExample(5, 0, 0, "measured"),
		timedArchiveExample(6, 60*time.Second, 60*time.Second, "measured"),
	}}
	fields, err := recorder.topicArchiveFields(state)
	if err != nil {
		t.Fatal(err)
	}
	assertTimingFields(t, fields, map[string]any{
		"耗时": float64(1911), "净处理秒数": float64(851), "等待秒数": float64(160),
		"有效计时轮数": float64(4), "慢回复数": float64(1),
		"平均单轮秒数": float64(212.75), "慢回复比例": float64(0.25),
		"交互轮数": float64(6),
	})
	if got := fields["归档数据"].(string); !strings.Contains(got, `"UserWaitTiming":"reconstructed"`) || !strings.Contains(got, `"UserWaitDuration":100000000000`) {
		t.Fatal("canonical state lost timing provenance or duration")
	}
}

func TestTopicArchive_TimingNumeratorsSupportWeightedDashboardTotals(t *testing.T) {
	_, recorder, fixture := newTopicArchiveFixture(t)
	enableTimingMetrics(t, recorder, fixture)
	one := []topicArchiveTurn{timedArchiveExample(1, 600*time.Second, 0, "measured")}
	three := []topicArchiveTurn{
		timedArchiveExample(1, 120*time.Second, 20*time.Second, "measured"),
		timedArchiveExample(2, 150*time.Second, 50*time.Second, "reconstructed"),
		timedArchiveExample(3, 100*time.Second, 0, "measured"),
	}
	var active, slow float64
	var turns int
	for _, records := range [][]topicArchiveTurn{one, three} {
		fields, err := recorder.topicArchiveFields(topicArchiveState{Version: 1, TaskID: "conversation_1", Turns: records})
		if err != nil {
			t.Fatal(err)
		}
		active += fields["净处理秒数"].(float64)
		slow += float64(fields["慢回复数"].(int))
		turns += fields["有效计时轮数"].(int)
	}
	if active/float64(turns) != 225 || slow/float64(turns) != 0.25 {
		t.Fatalf("weighted mean/ratio = %v/%v", active/float64(turns), slow/float64(turns))
	}
}

func TestTopicArchive_LegacyTimingIsUnknownAndRerendersNullMetrics(t *testing.T) {
	_, recorder, fixture := newTopicArchiveFixture(t)
	enableTimingMetrics(t, recorder, fixture)
	turn := timedArchiveExample(1, 900*time.Second, 0, "")
	state := topicArchiveState{Version: 1, TaskID: "conversation_1", Turns: []topicArchiveTurn{turn}}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	// Explicitly model the old writer, whose record did not contain either key.
	var old map[string]any
	if err := json.Unmarshal(encoded, &old); err != nil {
		t.Fatal(err)
	}
	record := old["turns"].([]any)[0].(map[string]any)["record"].(map[string]any)
	delete(record, "UserWaitDuration")
	delete(record, "UserWaitTiming")
	encoded, _ = json.Marshal(old)
	decoded, err := decodeTopicArchive(string(encoded), "conversation_1")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := recorder.topicArchiveFields(decoded)
	if err != nil {
		t.Fatal(err)
	}
	assertTimingFields(t, fields, map[string]any{
		"耗时": float64(900), "交互轮数": float64(1), "有效计时轮数": float64(0), "慢回复数": float64(0),
		"净处理秒数": nil, "等待秒数": nil, "平均单轮秒数": nil, "慢回复比例": nil,
	})
}

func TestTopicArchive_TimingRejectsInvalidProvenanceAndBoundsBeforeWrite(t *testing.T) {
	for _, scenario := range []struct {
		name, timing   string
		duration, wait time.Duration
	}{
		{"negative wait", "measured", time.Minute, -time.Second},
		{"wait exceeds total", "measured", time.Minute, time.Hour},
		{"missing total", "reconstructed", 0, time.Second},
		{"negative total", "measured", -time.Second, 0},
		{"unrecognized source", "assumed", time.Minute, 0},
		{"unknown partial invalid", "", time.Second, 2 * time.Second},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			p, recorder, fixture := newTopicArchiveFixture(t)
			enableTimingMetrics(t, recorder, fixture)
			turn := timedArchiveExample(1, scenario.duration, scenario.wait, scenario.timing)
			if err := recorder.writeTopic(context.Background(), p, "base_test", turn); err == nil {
				t.Fatal("accepted invalid timing")
			}
			if fixture.creates != 0 || fixture.updates != 0 {
				t.Fatal("invalid timing changed a remote record")
			}
		})
	}
}

func TestTopicArchive_TimingRetryAndRestartDoNotDoubleCount(t *testing.T) {
	p, recorder, fixture := newTopicArchiveFixture(t)
	enableTimingMetrics(t, recorder, fixture)
	first := timedArchiveExample(1, 360*time.Second, 90*time.Second, "measured")
	second := timedArchiveExample(2, 420*time.Second, 60*time.Second, "measured")
	for _, turn := range []topicArchiveTurn{first, second} {
		fixture.unknownWrite = 1
		if err := recorder.writeTopic(context.Background(), p, "base_test", turn); err != nil {
			t.Fatal(err)
		}
	}
	restarted := newTurnRecorder(recorder.cfg)
	defer restarted.stop()
	if err := restarted.writeTopic(context.Background(), p, "base_test", second); err != nil {
		t.Fatal(err)
	}
	if fixture.creates != 1 || fixture.updates != 1 {
		t.Fatalf("unexpected retry writes %d/%d", fixture.creates, fixture.updates)
	}
	assertTimingFields(t, fixture.rows["rec_1"], map[string]any{
		"净处理秒数": float64(630), "等待秒数": float64(150), "有效计时轮数": float64(2),
		"慢回复数": float64(1), "平均单轮秒数": float64(315), "慢回复比例": float64(0.5),
	})
	second.Record.UserWaitDuration++
	if err := restarted.writeTopic(context.Background(), p, "base_test", second); err == nil {
		t.Fatal("conflicting timing for the same native turn was silently overwritten")
	}
}

func TestTopicArchive_TimingColumnTypesAreValidated(t *testing.T) {
	for key, name := range timingMetricMapping() {
		t.Run(key, func(t *testing.T) {
			p, recorder, fixture := newTopicArchiveFixture(t)
			enableTimingMetrics(t, recorder, fixture)
			if fixture.types[name.(string)] != 2 {
				t.Fatal("metric is not a numeric column")
			}
			fixture.types[name.(string)] = 1
			if err := recorder.writeTopic(context.Background(), p, "base_test", timedArchiveExample(1, time.Minute, 0, "measured")); err == nil {
				t.Fatal("accepted nonnumeric timing column")
			}
			if fixture.creates != 0 {
				t.Fatal("wrong column type reached write")
			}
		})
	}
}

func TestTurnRecording_TimingMetricsAlsoSupportOneRowPerTurn(t *testing.T) {
	p, fixture := newTurnRecordFixture(t, map[string]any{"turn_record_fields": timingMetricMapping()})
	turn := timedArchiveExample(1, 400*time.Second, 100*time.Second, "measured")
	p.turnRecording.write(p, turn.Record)
	if len(fixture.recordReq) != 1 {
		t.Fatal("turn was not archived")
	}
	assertTimingFields(t, fixture.recordReq[0]["fields"].(map[string]any), map[string]any{
		"净处理秒数": float64(300), "等待秒数": float64(100), "有效计时轮数": float64(1),
		"慢回复数": float64(0), "平均单轮秒数": float64(300), "慢回复比例": float64(0),
	})
}

func TestParseTurnRecordTimingMappingsRequireCompleteDistinctGroup(t *testing.T) {
	if _, err := parseTurnRecordOptions(map[string]any{"turn_record_fields": timingMetricMapping()}); err != nil {
		t.Fatal(err)
	}
	for key := range timingMetricMapping() {
		for _, invalid := range []any{nil, "", 1, "问题"} {
			fields := timingMetricMapping()
			if invalid == nil {
				delete(fields, key)
			} else {
				fields[key] = invalid
			}
			if _, err := parseTurnRecordOptions(map[string]any{"turn_record_fields": fields}); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("invalid mapping %s=%v: %v", key, invalid, err)
			}
		}
	}
	fields := timingMetricMapping()
	fields["active_duration"] = fields["user_wait_duration"]
	if _, err := parseTurnRecordOptions(map[string]any{"turn_record_fields": fields}); err == nil {
		t.Fatal("accepted duplicated metric columns")
	}
	for key := range fields {
		fields[key] = " "
	}
	if cfg, err := parseTurnRecordOptions(map[string]any{"turn_record_fields": fields}); err != nil || cfg.fields.ActiveDuration != "" {
		t.Fatal("empty optional timing group changed legacy behavior")
	}
}

func TestTurnRecordTimingConfigContractDescribesWeightedDenominators(t *testing.T) {
	for _, option := range feishuConfigOptions("https://open.feishu.cn") {
		if option.Key != "turn_record_fields" {
			continue
		}
		for key := range timingMetricMapping() {
			if !strings.Contains(option.Description, key) || !strings.Contains(option.DescriptionZH, key) || !strings.Contains(option.Example, key) {
				t.Fatalf("contract missing metric %s", key)
			}
		}
		for _, description := range []string{option.Description, option.DescriptionZH} {
			if !strings.Contains(description, "SUM(active_duration)") || !strings.Contains(description, "SUM(timed_turn_count)") || !strings.Contains(description, "reconstructed") {
				t.Fatal("contract lacks weighted aggregation or timing provenance")
			}
		}
		return
	}
	t.Fatal("missing turn recording contract")
}
