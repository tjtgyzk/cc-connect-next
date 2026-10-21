package core

import "time"

// One accumulator belongs to one native turn. A steer changes presentation,
// not its clock; a queued or newly started turn gets a new accumulator. All
// fields below, including the pending request's intervals, use state.mu.
type turnUserWait struct {
	duration time.Duration
}

func (s *interactiveState) permissionBindingValidLocked(pending *pendingPermission) bool {
	return (pending.waitClock == nil || s.currentTurnUserWait == pending.waitClock) &&
		(pending.waitAgent == nil || s.agentSession == pending.waitAgent)
}

func (s *interactiveState) startPermissionWait(pending *pendingPermission, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startPermissionWaitLocked(pending, at)
}

func (s *interactiveState) startPermissionWaitLocked(pending *pendingPermission, at time.Time) {
	if s.pending != pending || pending.waitFinished || pending.responseClaimed || !pending.waitStartedAt.IsZero() ||
		pending.waitClock == nil || !s.permissionBindingValidLocked(pending) ||
		s.stopped || requestDone(pending.PermissionDone) {
		return
	}
	pending.waitPromptDelivered = true
	pending.waitStartedAt = at
}

// Stop at validated input arrival, before card switching or native RPC work.
// Segments remain provisional until the complete native response is accepted.
func (s *interactiveState) pausePermissionWaitLocked(pending *pendingPermission, at time.Time) {
	if !pending.waitFinished && s.permissionBindingValidLocked(pending) &&
		!pending.waitStartedAt.IsZero() && at.After(pending.waitStartedAt) {
		pending.waitDuration += at.Sub(pending.waitStartedAt)
	}
	pending.waitStartedAt = time.Time{}
}

func (s *interactiveState) finishPermissionWaitLocked(pending *pendingPermission, at time.Time) {
	if pending.waitFinished {
		return
	}
	s.pausePermissionWaitLocked(pending, at)
	pending.waitFinished = true
	if pending.waitClock == nil || !s.permissionBindingValidLocked(pending) || s.stopped ||
		(s.currentMessageID != "" && s.discardTurnMessageID == s.currentMessageID) {
		return
	}
	pending.waitClock.duration += pending.waitDuration
}

// Keep failed writes retryable while the native request remains open. Their
// native RPC time is not user wait; restart waiting when the failed write
// returns. Complete the attempt after audit changes so PermissionDone cannot
// overtake them or charge the write's processing time.
func (s *interactiveState) finishPermissionResponse(pending *pendingPermission, agent AgentSession, success, approveAll bool, at time.Time) {
	s.mu.Lock()
	attemptDone := pending.responseDone
	pending.responseDone = nil
	pending.responseClaimed = false
	current := s.pending == pending && s.agentSession == agent && s.permissionBindingValidLocked(pending) && !s.stopped &&
		(s.currentMessageID == "" || s.discardTurnMessageID != s.currentMessageID)
	resolve := success || !current || requestDone(pending.PermissionDone)
	if resolve {
		if current {
			s.finishPermissionWaitLocked(pending, at)
			if success && approveAll {
				s.approveAll = true
			}
		}
		if s.pending == pending {
			s.pending = nil
		}
	} else if pending.waitPromptDelivered {
		s.startPermissionWaitLocked(pending, at)
	}
	s.mu.Unlock()
	if resolve {
		pending.resolve()
	}
	if attemptDone != nil {
		close(attemptDone)
	}
}
