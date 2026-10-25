package relay_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.temporal.io/api/serviceerror"

	"github.com/k-sparrow/docling-serve-ray/src/callback_relay/relay"
)

const token = "s3cret"

// What the Python worker registers for workflow
// `convert:docling-input/incoming/a b.pdf` (urllib.parse.quote, safe="").
const callbackPath = "/v1/callbacks/convert%3Adocling-input%2Fincoming%2Fa%20b.pdf"

type signal struct {
	workflowID string
	runID      string
	name       string
	arg        interface{}
}

type fakeSignaler struct {
	err     error
	signals []signal
}

func (f *fakeSignaler) SignalWorkflow(_ context.Context, workflowID, runID, signalName string, arg interface{}) error {
	f.signals = append(f.signals, signal{workflowID, runID, signalName, arg})
	return f.err
}

func post(t *testing.T, signaler relay.Signaler, path, tok, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if tok != "" {
		req.Header.Set(relay.TokenHeader, tok)
	}
	rec := httptest.NewRecorder()
	relay.NewHandler(signaler, token, slog.New(slog.NewTextHandler(io.Discard, nil))).ServeHTTP(rec, req)
	return rec
}

func ptr(s string) *string { return &s }

func TestTaskCompletedSignalsTheWorkflowInThePath(t *testing.T) {
	tests := []struct {
		name string
		body string
		want relay.TaskCompleted
	}{
		{
			name: "success",
			body: `{"task_id": "t-1", "progress": {"kind": "task_completed", "task_status": "success", "failure": null}}`,
			want: relay.TaskCompleted{TaskID: "t-1", TaskStatus: "success"},
		},
		{
			name: "failure carries docling's message",
			body: `{"task_id": "t-1", "progress": {"kind": "task_completed", "task_status": "failure",
				"failure": {"category": "conversion", "message": "bad pdf", "retryable": false, "phase": "convert", "details": {}}}}`,
			want: relay.TaskCompleted{TaskID: "t-1", TaskStatus: "failure", ErrorMessage: ptr("bad pdf")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeSignaler{}

			rec := post(t, f, callbackPath, token, tt.body)

			if rec.Code != http.StatusOK || rec.Body.String() != `{"status":"ack"}` {
				t.Fatalf("got %d %q, want 200 ack", rec.Code, rec.Body.String())
			}
			if len(f.signals) != 1 {
				t.Fatalf("got %d signals, want 1", len(f.signals))
			}
			got := f.signals[0]
			if got.workflowID != "convert:docling-input/incoming/a b.pdf" || got.runID != "" || got.name != relay.SignalName {
				t.Errorf("signaled %q run %q with %q", got.workflowID, got.runID, got.name)
			}
			event, ok := got.arg.(relay.TaskCompleted)
			if !ok {
				t.Fatalf("signal arg is %T, want relay.TaskCompleted", got.arg)
			}
			if event.TaskID != tt.want.TaskID || event.TaskStatus != tt.want.TaskStatus ||
				(event.ErrorMessage == nil) != (tt.want.ErrorMessage == nil) ||
				(event.ErrorMessage != nil && *event.ErrorMessage != *tt.want.ErrorMessage) {
				t.Errorf("got %+v, want %+v", event, tt.want)
			}
		})
	}
}

func TestSignalPayloadMatchesThePythonContract(t *testing.T) {
	// models.TaskCompleted(task_id, task_status, error_message=None): the
	// field names must match, and an absent message must decode as None.
	tests := []struct {
		event relay.TaskCompleted
		want  string
	}{
		{relay.TaskCompleted{TaskID: "t-1", TaskStatus: "success"}, `{"task_id":"t-1","task_status":"success"}`},
		{
			relay.TaskCompleted{TaskID: "t-1", TaskStatus: "failure", ErrorMessage: ptr("bad pdf")},
			`{"task_id":"t-1","task_status":"failure","error_message":"bad pdf"}`,
		},
	}
	for _, tt := range tests {
		got, err := json.Marshal(tt.event)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tt.want {
			t.Errorf("got %s, want %s", got, tt.want)
		}
	}
}

func TestRejectsCallbacksWithoutTheToken(t *testing.T) {
	for name, tok := range map[string]string{"missing": "", "wrong": "guess"} {
		t.Run(name, func(t *testing.T) {
			f := &fakeSignaler{}

			rec := post(t, f, callbackPath, tok, `{"task_id": "t-1", "progress": {"kind": "task_completed", "task_status": "success"}}`)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("got %d, want 401", rec.Code)
			}
			if len(f.signals) != 0 {
				t.Errorf("signaled %d times, want 0", len(f.signals))
			}
		})
	}
}

func TestEmptyConfiguredTokenRejectsEverything(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, callbackPath, strings.NewReader(`{}`))
	rec := httptest.NewRecorder()

	relay.NewHandler(&fakeSignaler{}, "", slog.New(slog.NewTextHandler(io.Discard, nil))).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("got %d, want 401", rec.Code)
	}
}

func TestProgressEventsAreAcknowledgedWithoutSignaling(t *testing.T) {
	for _, kind := range []string{"set_num_docs", "update_processed", "document_completed"} {
		t.Run(kind, func(t *testing.T) {
			f := &fakeSignaler{}

			rec := post(t, f, callbackPath, token, `{"task_id": "t-1", "progress": {"kind": "`+kind+`"}}`)

			if rec.Code != http.StatusOK {
				t.Errorf("got %d, want 200", rec.Code)
			}
			if len(f.signals) != 0 {
				t.Errorf("signaled %d times, want 0", len(f.signals))
			}
		})
	}
}

func TestRejectsMalformedCallbacks(t *testing.T) {
	tests := map[string]string{
		"not json":       `{"task_id": `,
		"no task id":     `{"progress": {"kind": "task_completed", "task_status": "success"}}`,
		"unknown status": `{"task_id": "t-1", "progress": {"kind": "task_completed", "task_status": "started"}}`,
		"oversized":      `{"task_id": "` + strings.Repeat("x", 2<<20) + `"}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			f := &fakeSignaler{}

			rec := post(t, f, callbackPath, token, body)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("got %d, want 400", rec.Code)
			}
			if len(f.signals) != 0 {
				t.Errorf("signaled %d times, want 0", len(f.signals))
			}
		})
	}
}

func TestSignalErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		// Workflow already closed: nothing to wake, so no docling retry.
		{"workflow gone", serviceerror.NewNotFound("workflow execution already completed"), http.StatusOK},
		// Temporal down: docling should retry.
		{"temporal unavailable", errors.New("connection refused"), http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeSignaler{err: tt.err}

			rec := post(t, f, callbackPath, token, `{"task_id": "t-1", "progress": {"kind": "task_completed", "task_status": "success"}}`)

			if rec.Code != tt.want {
				t.Errorf("got %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()

	relay.NewHandler(&fakeSignaler{}, token, slog.New(slog.NewTextHandler(io.Discard, nil))).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusNoContent {
		t.Errorf("got %d, want 204", rec.Code)
	}
}
