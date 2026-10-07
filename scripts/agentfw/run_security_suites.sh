#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

echo "[agentfw] running security validation suites"
go test ./internal/agentfw/tool/... -run '^TestSecuritySuite' -count=1 -shuffle=off -timeout=10m
