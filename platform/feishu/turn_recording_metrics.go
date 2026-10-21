package feishu

import (
	"fmt"
	"time"

	"github.com/timmyagentic/cc-connect-next/core"
)

// These columns are a single optional group: keeping both numerators and the
// denominator allows dashboards to weight by turns instead of averaging topics.
func (f turnRecordFields) timingMappings() map[string]string {
	return map[string]string{
		"active_duration": f.ActiveDuration, "user_wait_duration": f.UserWaitDuration,
		"timed_turn_count": f.TimedTurnCount, "slow_turn_count": f.SlowTurnCount,
		"mean_turn_duration": f.MeanTurnDuration, "slow_turn_ratio": f.SlowTurnRatio,
	}
}

func (f turnRecordFields) validateTimingMappings() error {
	mappings := f.timingMappings()
	configured := 0
	for _, name := range mappings {
		if name != "" {
			configured++
		}
	}
	if configured == 0 || configured == len(mappings) {
		return nil
	}
	for key, name := range mappings {
		if name == "" {
			return fmt.Errorf("turn_record_fields.%s is required when configuring per-turn timing metrics", key)
		}
	}
	return nil
}

func validateTurnRecordTiming(rec core.TurnRecord) error {
	if rec.Duration < 0 || rec.UserWaitDuration < 0 || rec.UserWaitDuration > rec.Duration {
		return fmt.Errorf("feishu: turn timing requires 0 <= user wait <= total duration")
	}
	switch rec.UserWaitTiming {
	case "", "measured", "reconstructed":
		return nil
	default:
		return fmt.Errorf("feishu: turn timing provenance is invalid")
	}
}

type turnRecordTimingTotals struct {
	activeSeconds float64
	waitSeconds   float64
	timedTurns    int
	slowTurns     int
}

func (totals *turnRecordTimingTotals) add(rec core.TurnRecord) error {
	if err := validateTurnRecordTiming(rec); err != nil {
		return err
	}
	// Older version-1 records lack wait provenance. A missing measurement must
	// never be treated as proof that the user answered immediately.
	if rec.UserWaitTiming == "" || rec.Duration == 0 {
		return nil
	}
	active := rec.Duration - rec.UserWaitDuration
	totals.activeSeconds += active.Seconds()
	totals.waitSeconds += rec.UserWaitDuration.Seconds()
	totals.timedTurns++
	if active > 5*time.Minute {
		totals.slowTurns++
	}
	return nil
}

func (totals turnRecordTimingTotals) writeFields(fields map[string]any, f turnRecordFields) {
	if f.ActiveDuration == "" {
		return
	}
	fields[f.TimedTurnCount] = totals.timedTurns
	fields[f.SlowTurnCount] = totals.slowTurns
	// Explicit null clears stale derived metrics during a historical re-render.
	for _, name := range []string{f.ActiveDuration, f.UserWaitDuration, f.MeanTurnDuration, f.SlowTurnRatio} {
		fields[name] = nil
	}
	if totals.timedTurns == 0 {
		return
	}
	fields[f.ActiveDuration] = totals.activeSeconds
	fields[f.UserWaitDuration] = totals.waitSeconds
	fields[f.MeanTurnDuration] = totals.activeSeconds / float64(totals.timedTurns)
	fields[f.SlowTurnRatio] = float64(totals.slowTurns) / float64(totals.timedTurns)
}
