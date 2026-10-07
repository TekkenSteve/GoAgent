#!/bin/sh
# Provision the AgentOS event-backbone streams (NATS JetStream).
#
# Idempotent: existing streams are left alone, so running it on every stack
# start is safe. Same shape as scripts/temporal/create-namespace.sh.
#
# Why creation is explicit rather than let the broker auto-create: a stream's
# subjects, retention and duplicate window are the contract consumers rely on.
# Creating them lazily by publishing means the first writer silently decides
# retention for everyone.
#
# Stream names mirror pkg/eventlog/nats (Config.streamFor): the uppercased
# prefix, an underscore, and the domain with its dots as underscores. A
# mismatch would make the app publish into a stream nobody provisioned.

set -e

URL=${AGENTFW_NATS_URL:-nats://guest:guest@nats:4222}
PREFIX=${AGENTFW_NATS_SUBJECT_PREFIX:-agentos}
REPLICAS=${AGENTFW_NATS_REPLICAS:-1}

# Retention is AGE-based, never count-based: a count/byte bound truncates
# silently for a consumer that fell behind — the failure mode that rules out
# capacity-driven retention entirely.
MAX_AGE=${AGENTFW_NATS_MAX_AGE:-168h}
DLQ_MAX_AGE=${AGENTFW_NATS_DLQ_MAX_AGE:-720h}

# Duplicate window: broker-side dedupe on the publisher-supplied Nats-Msg-Id,
# which the drainer sets to the fact's event id. It is a TIME window, so it is
# a safety net rather than exactly-once; consumers stay idempotent regardless.
DUPE_WINDOW=${AGENTFW_NATS_DUPE_WINDOW:-2m}

# The logical domains of the fact log. One stream per domain, one shared
# dead-letter stream: poison records are one operational concern. The list must
# match pkg/eventlog's domain constants — a stream nobody declares is a stream
# nobody reads, and it silently retains whatever is published to it.
DOMAINS="run.timeline plan.events conversation.events commands"

echo "Waiting for NATS JetStream at $URL..."

attempt=0
until nats stream ls --server "$URL" >/dev/null 2>&1; do
  attempt=$((attempt + 1))

  if [ "$attempt" -ge 30 ]; then
    echo "NATS JetStream at $URL did not become ready; last error was:" >&2
    nats stream ls --server "$URL" >/dev/null || true
    exit 1
  fi

  sleep 2
done

echo "NATS JetStream at $URL is ready"

# stream_name is the uppercased prefix + domain, dots as underscores — the
# same name the clients compute from the same prefix.
stream_name() {
  echo "$PREFIX.$1" | tr '[:lower:]' '[:upper:]' | tr '.' '_'
}

create_stream() {
  name=$1
  subjects=$2
  max_age=$3

  if nats stream info "$name" --server "$URL" >/dev/null 2>&1; then
    echo "Stream '$name' already exists"
    return 0
  fi

  # Success stays quiet; failure carries its reason, so a rejected URL or
  # stream config names itself in the log instead of exiting blind.
  if out=$(nats stream add "$name" \
    --server "$URL" \
    --subjects "$subjects" \
    --storage file \
    --replicas "$REPLICAS" \
    --max-age "$max_age" \
    --dupe-window "$DUPE_WINDOW" \
    --discard old \
    --defaults 2>&1); then
    echo "Stream '$name' created (subjects=$subjects max-age=$max_age)"
  else
    echo "Failed to create stream '$name':" >&2
    echo "$out" >&2
    exit 1
  fi
}

echo 'Creating AgentOS event-backbone streams...'

for domain in $DOMAINS; do
  create_stream "$(stream_name "$domain")" "${PREFIX}.${domain}.>" "$MAX_AGE"
done

# One dead-letter stream for every domain: a poison record is moved aside
# rather than blocking a consumer's progress, and kept long enough to replay.
create_stream "$(stream_name dlq)" "${PREFIX}.dlq.>" "$DLQ_MAX_AGE"

echo 'AgentOS event-backbone streams are ready'
