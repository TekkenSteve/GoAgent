#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

repo_grep() {
  local pattern="$1"
  shift
  git -C "$ROOT_DIR" grep -n -E "$pattern" -- "$@"
}

if repo_grep 'github\.com/TekkenSteve/GoAgent/(agentfw|entity|repo|usecase|state)(/|")' \
  examples docs README.md README_CN.md README_RU.md; then
  echo "public examples/docs must not import old GoAgent implementation packages" >&2
  exit 1
fi

if repo_grep 'github\.com/TekkenSteve/GoAgent/pkg/(redis|postgres)(/|")' \
  examples docs README.md README_CN.md README_RU.md; then
  echo "public examples/docs must not import GoAgent runtime infrastructure packages" >&2
  exit 1
fi

if repo_grep 'github\.com/TekkenSteve/GoAgent/internal/' examples docs; then
  echo "public examples/docs must not import GoAgent internal packages" >&2
  exit 1
fi

if repo_grep 'github\.com/TekkenSteve/GoAgent/internal/(repo|agentfw)(/|")' \
  internal/usecase/agentosruntime; then
  echo "AgentOS runtime usecase must not import repo or agentfw infrastructure packages" >&2
  exit 1
fi

if repo_grep 'github\.com/TekkenSteve/GoAgent/internal/agentfw/(backend|eventing)(/|")' \
  internal/repo/agentos; then
  echo "AgentOS backend adapters must depend on usecase ports, not agentfw implementation packages" >&2
  exit 1
fi
