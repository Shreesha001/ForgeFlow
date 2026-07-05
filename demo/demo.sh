#!/usr/bin/env bash
# ForgeFlow demo: run after `docker compose up --build -d`.
# Submits shell jobs (no external URL needed) and an http job against the
# fake service below. Watch everything at http://localhost:8080
set -euo pipefail

API=${API:-localhost:8080}

echo "--- 1. immediate shell job (succeeds)"
curl -s -X POST "$API/api/jobs" -d '{
  "name": "hello",
  "executor": "shell",
  "payload": {"command": "echo hello from forgeflow"}
}' | python3 -m json.tool

echo "--- 2. recurring job: every minute"
curl -s -X POST "$API/api/jobs" -d '{
  "name": "tick",
  "executor": "shell",
  "cron_expr": "* * * * *",
  "payload": {"command": "date"}
}' | python3 -m json.tool

echo "--- 3. failing job: retries with backoff, then dead-letters"
curl -s -X POST "$API/api/jobs" -d '{
  "name": "doomed",
  "executor": "shell",
  "max_attempts": 3,
  "payload": {"command": "echo simulated failure >&2; exit 1"}
}' | python3 -m json.tool

echo
echo "Open http://$API to watch. Run history: curl $API/api/runs"
echo "For the http-job demo, first start the fake service:"
echo "  python3 demo/flaky-service.py     # then:"
echo "  bash demo/demo-http.sh"
