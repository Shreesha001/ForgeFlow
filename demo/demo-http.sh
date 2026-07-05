#!/usr/bin/env bash
# Submits an http job that calls the flaky service (start it first:
# python3 demo/flaky-service.py). ForgeFlow workers run inside Docker, so
# they reach your host via 172.17.0.1; override with TARGET if needed
# (e.g. TARGET=http://localhost:9000/run when ForgeFlow runs via `make run`).
set -euo pipefail

API=${API:-localhost:8080}
TARGET=${TARGET:-http://172.17.0.1:9000/run}

curl -s -X POST "$API/api/jobs" -d '{
  "name": "billing-demo",
  "executor": "http",
  "max_attempts": 5,
  "payload": {"url": "'"$TARGET"'", "body": {"period": "2026-07"}}
}' | python3 -m json.tool

echo
echo "Watch the flaky service terminal: 2 failures, then success (~15s total)."
echo "Watch the run retry live at http://$API"
