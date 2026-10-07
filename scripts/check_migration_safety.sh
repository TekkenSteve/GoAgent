#!/usr/bin/env bash
#
# Statement-level guard on the migration chain.
#
# golang-migrate runs each migration inside a transaction, and PostgreSQL
# refuses a handful of statements inside one. The failure is not subtle — the
# migration errors out, or waits on a lock it can never take, during a deploy,
# which is the worst moment to discover it. Statements that cannot run in a
# transaction belong in the online channel instead, applied by hand and
# documented in the runbook.
#
# The patterns match statements, not words: a migration comment explaining that
# two tenants write concurrently is not a DDL statement, and a guard that cannot
# tell the difference teaches people to ignore it.
set -euo pipefail

ROOT="${1:-.}"
cd "$ROOT"

FORBIDDEN='CREATE[[:space:]]+(UNIQUE[[:space:]]+)?INDEX[[:space:]]+CONCURRENTLY|DROP[[:space:]]+INDEX[[:space:]]+CONCURRENTLY|REINDEX[^;]*CONCURRENTLY|^[[:space:]]*VACUUM\b|^[[:space:]]*CREATE[[:space:]]+DATABASE\b|^[[:space:]]*DROP[[:space:]]+DATABASE\b|^[[:space:]]*ALTER[[:space:]]+SYSTEM\b'

MIGRATIONS="$(find migrations -maxdepth 1 -name '*.sql' | sort)"
if [ -z "$MIGRATIONS" ]; then
  echo "migration-safety: no migrations found" >&2
  exit 1
fi

violations=0

for file in $MIGRATIONS; do
  # Comments are stripped before matching, so prose cannot trip the guard.
  if matches="$(sed -E 's/--.*$//' "$file" | grep -nEi "$FORBIDDEN")"; then
    echo "migration-safety: $file uses a statement PostgreSQL cannot run in a transaction:" >&2
    echo "$matches" >&2
    violations=1
  fi
done

if [ "$violations" -ne 0 ]; then
  echo "migration-safety: move it to the online channel (docs/runbooks/online-index-migration.md)" >&2
  exit 1
fi

echo "migration-safety: pass ($(echo "$MIGRATIONS" | wc -l | tr -d ' ') migrations checked)"
