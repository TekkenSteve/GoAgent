#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"
cd "$ROOT"

if command -v rg >/dev/null 2>&1; then
  WORKFLOW_FILES="$(rg --files internal/agentfw | rg '/workflow(_test)?\.go$' || true)"
else
  WORKFLOW_FILES="$(find internal/agentfw -type f | grep -E '/workflow(_test)?\.go$' || true)"
fi
if [ -z "$WORKFLOW_FILES" ]; then
  echo "determinism-check: no workflow files found"
  exit 0
fi

# Forbidden imports in workflow files.
forbidden_imports='"net/http"|"database/sql"|"os"|"io/ioutil"|"math/rand"|"crypto/rand"'
if command -v rg >/dev/null 2>&1; then
  if rg -n --pcre2 "^\s*${forbidden_imports}" $WORKFLOW_FILES; then
    echo "determinism-check: forbidden imports found in workflow files" >&2
    exit 1
  fi
else
  if grep -nE "^[[:space:]]*${forbidden_imports}" $WORKFLOW_FILES; then
    echo "determinism-check: forbidden imports found in workflow files" >&2
    exit 1
  fi
fi

# Forbidden calls in workflow files.
forbidden_calls='\btime\.Now\s*\(|\brand\.\w+\s*\(|\bos\.Getenv\s*\(|\bhttp\.(Get|Post|Do)\s*\(|\bsql\.Open\s*\('
if command -v rg >/dev/null 2>&1; then
  if rg -n --pcre2 "$forbidden_calls" $WORKFLOW_FILES; then
    echo "determinism-check: forbidden non-deterministic calls found in workflow files" >&2
    exit 1
  fi
else
  if grep -nE "time\.Now[[:space:]]*\(|rand\.[a-zA-Z_][a-zA-Z0-9_]*[[:space:]]*\(|os\.Getenv[[:space:]]*\(|http\.(Get|Post|Do)[[:space:]]*\(|sql\.Open[[:space:]]*\(" $WORKFLOW_FILES; then
    echo "determinism-check: forbidden non-deterministic calls found in workflow files" >&2
    exit 1
  fi
fi

echo "determinism-check: pass"
