package codex

// This opt-in integration test runs the actual Codex executable and this Go
// adapter. Only the model is a deterministic loopback Responses server; it uses
// no account credentials and makes no business query or Feishu request.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/timmyagentic/cc-connect-next/core"
)

func TestNativeAppServerQuestionLifecycle(t *testing.T) {
	for _, selected := range []string{"prod", "stg"} {
		t.Run(selected, func(t *testing.T) { runNativeAppServerQuestionLifecycle(t, selected) })
	}
}

func runNativeAppServerQuestionLifecycle(t *testing.T, selected string) {
	binary := os.Getenv("CC_CONNECT_CODEX_NATIVE_BIN")
	if binary == "" {
		t.Skip("set CC_CONNECT_CODEX_NATIVE_BIN to run the real isolated Codex integration")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(version)) != "codex-cli 0.158.0" {
		t.Fatalf("expected audited Codex 0.158.0, got %q", version)
	}
	base := t.TempDir()
	project, home := filepath.Join(base, "project"), filepath.Join(base, "home")
	for _, path := range []string{project, home, filepath.Join(project, ".git")} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	requests := 0
	var answers []string
	var modelErrors []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/responses") {
			modelErrors = append(modelErrors, "unexpected model request")
			http.Error(w, "unexpected", 500)
			return
		}
		var body struct {
			Input []map[string]any `json:"input"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&body); err != nil {
			modelErrors = append(modelErrors, err.Error())
			http.Error(w, "bad request", 500)
			return
		}
		for _, item := range body.Input {
			if item["type"] == "function_call_output" {
				if output, ok := item["output"].(string); ok {
					answers = append(answers, output)
				}
			}
		}
		index := requests
		requests++
		id := fmt.Sprintf("resp-hitl-%d", index)
		events := []map[string]any{{"type": "response.created", "response": map[string]any{"id": id}}}
		var item map[string]any
		if index == 0 || index == 2 {
			arguments, _ := json.Marshal(map[string]any{"questions": []any{map[string]any{
				"id": "environment", "header": "Environment", "question": "Which environment should be queried?",
				"options": []any{map[string]any{"label": "prod", "description": "Production"}, map[string]any{"label": "stg", "description": "Staging"}},
			}}})
			item = map[string]any{"type": "function_call", "call_id": fmt.Sprintf("hitl-call-%d", index), "name": "request_user_input", "arguments": string(arguments)}
		} else {
			item = map[string]any{"type": "message", "role": "assistant", "id": "msg-hitl", "content": []any{map[string]any{"type": "output_text", "text": "Selected " + selected + ". No business query was performed."}}}
		}
		events = append(events, map[string]any{"type": "response.output_item.done", "item": item}, map[string]any{
			"type": "response.completed", "response": map[string]any{"id": id, "usage": map[string]int{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}},
		})
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range events {
			data, _ := json.Marshal(event)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event["type"], data)
		}
	}))
	defer server.Close()
	config := fmt.Sprintf(`model = "gpt-5.4"
model_provider = "hitl-smoke"
approval_policy = "never"
sandbox_mode = "danger-full-access"
[features]
default_mode_request_user_input = true
shell_snapshot = false
[analytics]
enabled = false
[model_providers.hitl-smoke]
name = "Isolated native HITL smoke"
base_url = %q
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0
[projects.%q]
trust_level = "trusted"
`, server.URL+"/v1", project)
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	// The executable is launched with an isolated CODEX_HOME and user home. The
	// documented invocation additionally runs this test binary under env -i.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	s, err := newAppServerSession(ctx, appServerSessionParams{
		cliBin: binary, workDir: project, model: "gpt-5.4", modelProvider: "hitl-smoke", mode: "yolo", codexHome: home,
		extraEnv: []string{"HOME=" + home, "NO_PROXY=127.0.0.1,localhost", "HTTP_PROXY=", "HTTPS_PROXY=", "ALL_PROXY="},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.initialTitleHandled = true
	thread := s.CurrentSessionID()
	if err := s.Send("Please ask whether to query prod or stg before proceeding. Do not run any business query.", nil, nil); err != nil {
		t.Fatal(err)
	}
	question := waitNativeQuestionEvent(t, s, core.EventPermissionRequest)
	turn, _ := question.ToolInputRaw["turnId"].(string)
	if question.ToolInputRaw["threadId"] != thread || turn == "" || question.RequestID == "" || len(question.Questions) != 1 || question.Questions[0].ID != "environment" {
		t.Fatalf("incomplete native routing: %#v", question)
	}
	waitDeadline := time.NewTimer(400 * time.Millisecond)
waitForHuman:
	for {
		select {
		case <-question.PermissionDone:
			t.Fatal("native question resolved before human answer")
		case event := <-s.Events():
			if event.Type == core.EventResult || event.Type == core.EventError {
				t.Fatalf("turn continued before answer: %#v", event)
			}
		case <-waitDeadline.C:
			break waitForHuman
		}
	}

	mu.Lock()
	count := requests
	mu.Unlock()
	if count != 1 {
		t.Fatalf("model continued while waiting: requests=%d", count)
	}
	result := core.PermissionResult{Behavior: "allow", UpdatedInput: map[string]any{"answers": map[string]any{"environment": selected}}}
	if err := s.RespondPermission(question.RequestID, result); err != nil {
		t.Fatal(err)
	}
	if err := s.RespondPermission(question.RequestID, result); err == nil {
		t.Fatal("duplicate human answer was accepted")
	}
	waitNativeQuestionEvent(t, s, core.EventResult)
	select {
	case <-question.PermissionDone:
	default:
		t.Fatal("answered question lifecycle still open")
	}
	if s.CurrentSessionID() != thread {
		t.Fatal("answer switched threads")
	}
	var snapshot struct {
		Thread struct {
			ID    string `json:"id"`
			Turns []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"turns"`
		} `json:"thread"`
	}
	if err := s.request("thread/read", map[string]any{"threadId": thread, "includeTurns": true}, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Thread.ID != thread || len(snapshot.Thread.Turns) != 1 || snapshot.Thread.Turns[0].ID != turn || snapshot.Thread.Turns[0].Status != "completed" {
		t.Fatalf("answer did not continue original turn: %#v", snapshot)
	}
	mu.Lock()
	outputs := append([]string(nil), answers...)
	errs := append([]string(nil), modelErrors...)
	mu.Unlock()
	if len(errs) > 0 || len(outputs) != 1 {
		t.Fatalf("model transport errors=%v outputs=%v", errs, outputs)
	}
	var returned appServerRequestUserInputResponse
	if err := json.Unmarshal([]byte(outputs[0]), &returned); err != nil {
		t.Fatal(err)
	}
	if got := returned.Answers["environment"].Answers; len(got) != 1 || got[0] != selected {
		t.Fatalf("wrong answer reached native tool: %#v", returned)
	}

	// The next real native turn is interrupted while waiting. It must invalidate
	// the UI capability instead of inventing an empty answer and continuing.
	if err := s.Send("Ask prod or stg again; this isolated turn will be canceled.", nil, nil); err != nil {
		t.Fatal(err)
	}
	question2 := waitNativeQuestionEvent(t, s, core.EventPermissionRequest)
	turn2, _ := question2.ToolInputRaw["turnId"].(string)
	if err := s.request("turn/interrupt", map[string]any{"threadId": thread, "turnId": turn2}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-question2.PermissionDone:
	case <-ctx.Done():
		t.Fatal("native interrupt did not invalidate pending question")
	}
	if err := s.RespondPermission(question2.RequestID, result); err == nil {
		t.Fatal("answer to interrupted native request was accepted")
	}
	waitNativeQuestionEvent(t, s, core.EventResult)
	mu.Lock()
	count = requests
	mu.Unlock()
	if count != 3 {
		t.Fatalf("interrupted question resumed model: requests=%d", count)
	}
	t.Logf("native=%s adapter=real waiting=verified response_id=%s thread=%s turn=%s same_turn=true duplicate=rejected interrupt=invalidation model=loopback business_queries=0", strings.TrimSpace(string(version)), question.RequestID, thread, turn)
}

func waitNativeQuestionEvent(t *testing.T, s *appServerSession, want core.EventType) core.Event {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case event, open := <-s.Events():
			if !open {
				t.Fatal("adapter event stream closed")
			}
			if event.Type == core.EventError {
				t.Fatalf("native turn error: %v", event.Error)
			}
			if event.Type == want {
				return event
			}
		case <-deadline.C:
			t.Fatalf("timeout waiting for %s", want)
		}
	}
}
