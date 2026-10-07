#!/bin/sh
# Provision cross-cell fact replication (NATS JetStream Mirror / Source).
#
# A cell is a full deployment, not a tenant of a shared bus: it owns its facts
# and reads a friend's through replication. Two shapes, chosen per domain:
#
#   mirror  a read-only exact replica under the domain's own stream name. Use
#           it for a domain this cell does NOT write — and never publish to it
#           (a mirror accepts no direct publishes).
#   source  merge a friend's facts into this cell's own stream. Use it when
#           this cell also writes the domain; consumers then see both.
#
# One rule decides which one is correct: a domain has exactly one writing cell.
# Mirroring a domain you also write would give you two writers of one truth.
#
# The friend's JetStream API must be reachable from here: link the cells with a
# leafnode so $JS.<friend-domain>.API resolves. A leafnode shares the ordinary
# subject space too, so a mirrored domain is safe (it stores nothing published
# locally) while an aggregated one can see a fact come back — consumers stay
# idempotent on (entity, seq), which is the contract anyway.
#
# Idempotent: an existing mirror is left alone, an existing local stream gains
# the source. Same shape as scripts/nats/create-streams.sh.
#
# Environment:
#   AGENTFW_NATS_URL                  local server (default nats://guest:guest@nats:4222)
#   AGENTFW_NATS_SUBJECT_PREFIX       default agentos
#   AGENTFW_NATS_UPSTREAM_JS_DOMAIN   friend cell's JetStream domain (required)
#   AGENTFW_NATS_UPSTREAM_SUBJECT_PREFIX
#                                     friend cell's subject prefix (default: this
#                                     cell's). Set it when the two cells namespace
#                                     their subjects apart; a transform is then
#                                     derived so consumers here keep reading
#                                     their own prefix.
#   AGENTFW_NATS_MIRROR_STREAMS       domains to replicate read-only
#   AGENTFW_NATS_SOURCE_STREAMS       domains to aggregate into the local stream
#   AGENTFW_NATS_REPLICA_MAX_AGE      retention of a mirror stream (default 168h)
#   AGENTFW_NATS_REPLICAS             default 1
#   AGENTFW_NATS_DUPE_WINDOW          default 2m
#
# The upstream stream is addressed by the name the same prefix derives here, so
# two cells that keep the default prefix replicate without a mapping table.

set -e

URL=${AGENTFW_NATS_URL:-nats://guest:guest@nats:4222}
PREFIX=${AGENTFW_NATS_SUBJECT_PREFIX:-agentos}
UPSTREAM_PREFIX=${AGENTFW_NATS_UPSTREAM_SUBJECT_PREFIX:-$PREFIX}
UPSTREAM=${AGENTFW_NATS_UPSTREAM_JS_DOMAIN:-}
MIRRORS=${AGENTFW_NATS_MIRROR_STREAMS:-}
SOURCES=${AGENTFW_NATS_SOURCE_STREAMS:-}
REPLICA_MAX_AGE=${AGENTFW_NATS_REPLICA_MAX_AGE:-168h}
REPLICAS=${AGENTFW_NATS_REPLICAS:-1}
DUPE_WINDOW=${AGENTFW_NATS_DUPE_WINDOW:-2m}

if [ -z "$UPSTREAM" ]; then
  echo "AGENTFW_NATS_UPSTREAM_JS_DOMAIN is required: it names the friend cell to replicate from" >&2
  exit 1
fi

if [ -z "$MIRRORS" ] && [ -z "$SOURCES" ]; then
  echo "Nothing to do: set AGENTFW_NATS_MIRROR_STREAMS and/or AGENTFW_NATS_SOURCE_STREAMS" >&2
  exit 1
fi

# stream_name mirrors pkg/eventlog/nats (Config.streamFor): the uppercased
# prefix, an underscore, and the domain with its dots as underscores.
#
# The prefix matters on both sides: the replica's own stream must be named the
# way this cell derives it (so its consumers find it), while the upstream stream
# is named the way the friend's cell derives it.
stream_name() {
  echo "$1.$2" | tr '[:lower:]' '[:upper:]' | tr '.' '_'
}

# transform_json prints the subject transform a cross-prefix link needs: the
# friend publishes under its prefix, and this cell's consumers read their own.
# Same prefix means no transform at all — the measured default topology.
transform_json() {
  if [ "$UPSTREAM_PREFIX" = "$PREFIX" ]; then
    return 0
  fi

  printf ',\n  "subject_transform": { "src": "%s.%s.>", "dest": "%s.%s.>" }' \
    "$UPSTREAM_PREFIX" "$1" "$PREFIX" "$1"
}

# duration_ns turns the "168h" shape used by the other scripts into the
# nanosecond integer a stream config carries. The Go struct is what the CLI
# unmarshals, and it does not accept a duration string.
duration_ns() {
  hours=$(echo "$1" | tr -d 'hH')
  awk -v h="$hours" 'BEGIN { printf "%.0f", h * 3600 * 1000000000 }'
}

MAX_AGE_NS=$(duration_ns "$REPLICA_MAX_AGE")
DUPE_WINDOW_NS=$(duration_ns "$DUPE_WINDOW")

API_PREFIX="\$JS.$UPSTREAM.API"

is_mirror() {
  nats stream info "$1" --server "$URL" 2>/dev/null | grep -q "Mirror: "
}

create_mirror() {
  domain=$1
  name=$(stream_name "$PREFIX" "$domain")
  upstream_name=$(stream_name "$UPSTREAM_PREFIX" "$domain")

  if nats stream info "$name" --server "$URL" >/dev/null 2>&1; then
    if is_mirror "$name"; then
      echo "Stream '$name' is already a mirror"
      return 0
    fi

    echo "Stream '$name' already exists as a local stream; a mirror would replace it." >&2
    echo "Aggregate instead: put '$domain' in AGENTFW_NATS_SOURCE_STREAMS, or remove the stream deliberately." >&2

    return 1
  fi

  config=/tmp/cell-mirror.json

  cat > "$config" <<JSON
{
  "name": "$name",
  "subjects": [],
  "retention": "limits",
  "max_age": $MAX_AGE_NS,
  "storage": "file",
  "discard": "old",
  "num_replicas": $REPLICAS,
  "duplicate_window": $DUPE_WINDOW_NS,
  "mirror": {
    "name": "$upstream_name",
    "external": { "api": "$API_PREFIX" }
  }$(transform_json "$domain")
}
JSON

  nats stream add --config "$config" --server "$URL" >/dev/null
  echo "Stream '$name' created as a read-only mirror of $UPSTREAM"
}

create_source() {
  domain=$1
  name=$(stream_name "$PREFIX" "$domain")
  upstream_name=$(stream_name "$UPSTREAM_PREFIX" "$domain")
  config=/tmp/cell-source.json

  cat > "$config" <<JSON
{
  "name": "$name",
  "subjects": ["$PREFIX.$domain.>"],
  "retention": "limits",
  "max_age": $(duration_ns "${AGENTFW_NATS_MAX_AGE:-168h}"),
  "storage": "file",
  "discard": "old",
  "num_replicas": $REPLICAS,
  "duplicate_window": $DUPE_WINDOW_NS,
  "sources": [
    { "name": "$upstream_name", "external": { "api": "$API_PREFIX" } }$(transform_json "$domain")
  ]
}
JSON

  if nats stream info "$name" --server "$URL" >/dev/null 2>&1; then
    if is_mirror "$name"; then
      echo "Stream '$name' is a mirror; a stream cannot both mirror and aggregate." >&2
      echo "Remove it deliberately first if '$domain' should be aggregated instead." >&2

      return 1
    fi

    nats stream update "$name" --config "$config" --server "$URL" -f >/dev/null
    echo "Stream '$name' now aggregates $UPSTREAM"
  else
    nats stream add --config "$config" --server "$URL" >/dev/null
    echo "Stream '$name' created aggregating $UPSTREAM"
  fi
}

echo "Waiting for NATS at $URL..."

attempt=0
until nats stream ls --server "$URL" >/dev/null 2>&1; do
  attempt=$((attempt + 1))

  if [ "$attempt" -ge 30 ]; then
    echo "NATS at $URL did not become ready" >&2
    exit 1
  fi

  sleep 2
done

for domain in $MIRRORS; do
  create_mirror "$domain"
done

for domain in $SOURCES; do
  create_source "$domain"
done

echo 'Cross-cell replication is ready'
