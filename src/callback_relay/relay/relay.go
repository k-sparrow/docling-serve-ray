// Package relay turns docling-serve's task-completion callbacks into the
// Temporal signal that wakes the ConvertDocumentWorkflow sleeping on that
// task (src/orchestration/workflows.py).
//
// The worker registers one callback per submit, addressed to
// /v1/callbacks/{workflow id}, so the relay needs no state of its own: the
// URL names the workflow to signal, the payload names the task. docling
// delivers best-effort (3 attempts over ~3s), which is why the workflow
// keeps safety-polling regardless -- the relay only makes the common case
// fast.
package relay

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"go.temporal.io/api/serviceerror"
)

// SignalName and TaskCompleted's JSON field names are the contract with the
// Python workflow: models.TASK_COMPLETED_SIGNAL and models.TaskCompleted.
const SignalName = "task_completed"

// TokenHeader carries the shared secret the worker attaches to every
// callback it registers (its ORCH_CALLBACK_TOKEN).
const TokenHeader = "X-Callback-Token"

const (
	maxBodyBytes = 1 << 20
	// docling's callback client gives up after 30s; answer well before it.
	signalTimeout = 10 * time.Second

	kindTaskCompleted = "task_completed"
	statusSuccess     = "success"
	statusFailure     = "failure"
)

// TaskCompleted is the signal's argument. The SDK's default data converter
// sends it as JSON, which the workflow decodes into models.TaskCompleted.
type TaskCompleted struct {
	TaskID       string  `json:"task_id"`
	TaskStatus   string  `json:"task_status"`
	ErrorMessage *string `json:"error_message,omitempty"`
}

// Signaler is the one Temporal client call the relay makes;
// go.temporal.io/sdk/client.Client satisfies it.
type Signaler interface {
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error
}

// progressCallback is docling's ProgressCallbackRequest
// (docling.datamodel.service.callbacks), reduced to the fields read here.
type progressCallback struct {
	TaskID   string `json:"task_id"`
	Progress struct {
		Kind       string `json:"kind"`
		TaskStatus string `json:"task_status"`
		Failure    *struct {
			Message string `json:"message"`
		} `json:"failure"`
	} `json:"progress"`
}

// NewHandler serves POST /v1/callbacks/{workflowID} and GET /healthz. The
// token must be non-empty: an empty one would match a request that sends no
// token at all.
func NewHandler(signaler Signaler, token string, logger *slog.Logger) http.Handler {
	h := &handler{signaler: signaler, token: []byte(token), logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/callbacks/{workflowID}", h.callback)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

type handler struct {
	signaler Signaler
	token    []byte
	logger   *slog.Logger
}

func (h *handler) callback(w http.ResponseWriter, r *http.Request) {
	// ServeMux matches and unescapes per segment, so the %2F-escaped
	// slashes of a `convert:{bucket}/{key}` id stay inside this one value.
	workflowID := r.PathValue("workflowID")
	if len(h.token) == 0 || subtle.ConstantTimeCompare([]byte(r.Header.Get(TokenHeader)), h.token) != 1 {
		// Logged because nothing else would show it: a token that differs
		// from the worker's silently leaves every workflow to its safety
		// polls.
		h.logger.Warn("rejected callback with an invalid token", "workflow_id", workflowID)
		http.Error(w, "invalid callback token", http.StatusUnauthorized)
		return
	}

	var cb progressCallback
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&cb); err != nil {
		http.Error(w, "malformed callback: "+err.Error(), http.StatusBadRequest)
		return
	}
	// docling also reports per-document progress; only the task's terminal
	// event wakes the workflow.
	if cb.Progress.Kind != kindTaskCompleted {
		ack(w)
		return
	}
	if cb.TaskID == "" || (cb.Progress.TaskStatus != statusSuccess && cb.Progress.TaskStatus != statusFailure) {
		http.Error(w, "task_completed callback without a task id or terminal status", http.StatusBadRequest)
		return
	}

	event := TaskCompleted{TaskID: cb.TaskID, TaskStatus: cb.Progress.TaskStatus}
	if cb.Progress.Failure != nil {
		event.ErrorMessage = &cb.Progress.Failure.Message
	}
	log := h.logger.With("workflow_id", workflowID, "task_id", event.TaskID, "task_status", event.TaskStatus)

	ctx, cancel := context.WithTimeout(r.Context(), signalTimeout)
	defer cancel()
	err := h.signaler.SignalWorkflow(ctx, workflowID, "", SignalName, event)
	var notFound *serviceerror.NotFound
	switch {
	case err == nil:
		log.Info("signaled workflow")
	case errors.As(err, &notFound):
		// Closed or never started: nothing left to wake, and a docling retry
		// wouldn't change that.
		log.Warn("no running workflow to signal", "error", err)
	default:
		// Temporal unreachable: a 503 makes docling retry. Should its retries
		// run out too, the workflow's next safety poll finds the result.
		log.Error("signaling workflow failed", "error", err)
		http.Error(w, "temporal unavailable", http.StatusServiceUnavailable)
		return
	}
	ack(w)
}

// ack answers in docling's own ProgressCallbackResponse shape.
func ack(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ack"}`))
}
