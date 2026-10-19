package core

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestQuestionLifecycleChannelsStayOutOfSerializedTransport(t *testing.T) {
	for _, value := range []any{
		Event{Type: EventPermissionRequest, RequestID: "native-request", PermissionDone: make(chan struct{})},
		Message{Content: "askq:bound:0:1", IsQuestionResponse: true, QuestionResponse: make(chan QuestionResponseResult, 1)},
	} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("in-process acknowledgement broke transport serialization: %v", err)
		}
		for _, field := range []string{`"PermissionDone"`, `"QuestionResponse"`} {
			if strings.Contains(string(data), field) {
				t.Fatalf("runtime-only channel escaped onto transport: %s", data)
			}
		}
	}
}
