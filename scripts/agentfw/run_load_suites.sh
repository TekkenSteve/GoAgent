#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

echo "[agentfw] running reproducible load suites"
go test ./agentfw/tool/... ./agentfw/orchestration/... ./agentfw/runtimeops/... -run '^TestLoadSuite' -count=1 -shuffle=off -timeout=10m
