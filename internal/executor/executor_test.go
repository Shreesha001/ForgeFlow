package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestShellSuccess(t *testing.T) {
	out, err := Execute(context.Background(), "shell", json.RawMessage(`{"command":"echo hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "hello" {
		t.Errorf("out = %q, want hello", out)
	}
}

func TestShellFailureReturnsOutput(t *testing.T) {
	out, err := Execute(context.Background(), "shell", json.RawMessage(`{"command":"echo oops >&2; exit 3"}`))
	if err == nil {
		t.Fatal("want error for exit 3")
	}
	if !strings.Contains(out, "oops") {
		t.Errorf("stderr not captured: %q", out)
	}
}

func TestShellTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := Execute(ctx, "shell", json.RawMessage(`{"command":"sleep 5"}`)); err == nil {
		t.Fatal("want error for timed-out command")
	}
}

func TestShellRejectsBadPayload(t *testing.T) {
	if _, err := Execute(context.Background(), "shell", json.RawMessage(`{}`)); err == nil {
		t.Fatal("want error for missing command")
	}
}

func TestHTTPSuccessAndFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Write([]byte("accepted"))
	}))
	defer srv.Close()

	out, err := Execute(context.Background(), "http", json.RawMessage(`{"url":"`+srv.URL+`","body":{"x":1}}`))
	if err != nil || out != "accepted" {
		t.Fatalf("out=%q err=%v, want accepted/nil", out, err)
	}
	if _, err := Execute(context.Background(), "http", json.RawMessage(`{"url":"`+srv.URL+`/fail"}`)); err == nil {
		t.Fatal("want error for 500 response")
	}
}

func TestUnknownExecutor(t *testing.T) {
	if _, err := Execute(context.Background(), "carrier-pigeon", json.RawMessage(`{}`)); err == nil {
		t.Fatal("want error for unknown executor")
	}
}
