// Package executor runs job payloads. Two executors ship built in:
//
//   - shell: {"command": "..."} is run via sh -c
//   - http:  {"url": "...", "body": {...}} is POSTed; non-2xx is a failure
//
// Both respect the run's timeout through the context.
package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
)

const outputLimit = 64 * 1024 // stored output is truncated to keep rows small

// Execute runs a payload with the named executor and returns its output.
func Execute(ctx context.Context, executor string, payload json.RawMessage) (string, error) {
	switch executor {
	case "shell":
		return execShell(ctx, payload)
	case "http":
		return execHTTP(ctx, payload)
	default:
		return "", fmt.Errorf("unknown executor %q", executor)
	}
}

func execShell(ctx context.Context, payload json.RawMessage) (string, error) {
	var p struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.Command == "" {
		return "", fmt.Errorf("shell payload needs {\"command\": \"...\"}")
	}
	out, err := exec.CommandContext(ctx, "sh", "-c", p.Command).CombinedOutput()
	if len(out) > outputLimit {
		out = out[:outputLimit]
	}
	if err != nil {
		return string(out), fmt.Errorf("command failed: %w", err)
	}
	return string(out), nil
}

func execHTTP(ctx context.Context, payload json.RawMessage) (string, error) {
	var p struct {
		URL  string          `json:"url"`
		Body json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.URL == "" {
		return "", fmt.Errorf("http payload needs {\"url\": \"...\"}")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(p.Body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, outputLimit))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return string(body), fmt.Errorf("http status %d", resp.StatusCode)
	}
	return string(body), nil
}
