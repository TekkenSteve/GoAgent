#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if rg 'github\.com/TekkenSteve/GoAgent/(agentfw|entity|repo|usecase|state)(/|")' \
  "$ROOT_DIR/examples" "$ROOT_DIR/docs" "$ROOT_DIR/README.md" "$ROOT_DIR/README_CN.md" "$ROOT_DIR/README_RU.md"; then
  echo "public examples/docs must not import old GoAgent implementation packages" >&2
  exit 1
fi

if rg 'github\.com/TekkenSteve/GoAgent/pkg/(redis|postgres)(/|")' \
  "$ROOT_DIR/examples" "$ROOT_DIR/docs" "$ROOT_DIR/README.md" "$ROOT_DIR/README_CN.md" "$ROOT_DIR/README_RU.md"; then
  echo "public examples/docs must not import GoAgent runtime infrastructure packages" >&2
  exit 1
fi

if rg 'github\.com/TekkenSteve/GoAgent/internal/' "$ROOT_DIR/examples" "$ROOT_DIR/docs"; then
  echo "public examples/docs must not import GoAgent internal packages" >&2
  exit 1
fi

if rg 'github\.com/TekkenSteve/GoAgent/internal/(repo|agentfw)(/|")' \
  "$ROOT_DIR/internal/usecase/agentosruntime"; then
  echo "AgentOS runtime usecase must not import repo or agentfw infrastructure packages" >&2
  exit 1
fi

if rg 'github\.com/TekkenSteve/GoAgent/internal/agentfw/(backend|eventing)(/|")' \
  "$ROOT_DIR/internal/repo/agentos"; then
  echo "AgentOS backend adapters must depend on usecase ports, not agentfw implementation packages" >&2
  exit 1
fi
