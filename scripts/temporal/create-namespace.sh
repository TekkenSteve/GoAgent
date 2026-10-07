#!/bin/sh
set -eu

NAMESPACE=${DEFAULT_NAMESPACE:-default}
TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-temporal:7233}
# Search attributes used by GoAgent workflows
# (internal/agentfw/orchestration/search_attributes.go). The server has no
# dynamic-config key to auto-register these; they are registered against the
# cluster metadata here, on every `docker compose up` (idempotent).
SEARCH_ATTRIBUTES="goagent.run_id:Keyword goagent.lifecycle_state:Keyword goagent.model:Keyword goagent.iteration:Int goagent.last_tool:Keyword"
MAX_ATTEMPTS=${TEMPORAL_HEALTH_CHECK_MAX_ATTEMPTS:-30}
SLEEP_SECONDS=${TEMPORAL_HEALTH_CHECK_SLEEP_SECONDS:-5}

echo "Waiting for Temporal server port to be available..."
SERVER_HOST=$(echo "$TEMPORAL_ADDRESS" | cut -d: -f1)
SERVER_PORT=$(echo "$TEMPORAL_ADDRESS" | cut -d: -f2)
attempt=1
while ! nc -z -w 10 "$SERVER_HOST" "$SERVER_PORT"; do
  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "Temporal server port did not become available after $MAX_ATTEMPTS attempts"
    exit 1
  fi

  echo "Temporal server port not ready yet, waiting... (attempt $attempt/$MAX_ATTEMPTS)"
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECONDS"
done
echo 'Temporal server port is available'

echo 'Waiting for Temporal server to be healthy...'
attempt=1

while :; do
  if temporal operator cluster health --address "$TEMPORAL_ADDRESS"; then
    break
  fi

  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "Server did not become healthy after $MAX_ATTEMPTS attempts"
    exit 1
  fi

  echo "Server not ready yet, waiting... (attempt $attempt/$MAX_ATTEMPTS)"
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECONDS"
done

echo "Server is healthy, creating namespace '$NAMESPACE'..."

attempt=1
while :; do
  if temporal operator namespace describe -n "$NAMESPACE" --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1; then
    echo "Namespace '$NAMESPACE' already exists"
    break
  fi

  if temporal operator namespace create -n "$NAMESPACE" --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1; then
    echo "Namespace '$NAMESPACE' created"
    break
  fi

  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "Failed to create namespace '$NAMESPACE' after $MAX_ATTEMPTS attempts"
    exit 1
  fi

  echo "Namespace operation not ready yet, waiting... (attempt $attempt/$MAX_ATTEMPTS)"
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECONDS"
done

echo "Registering GoAgent search attributes..."

for sa in $SEARCH_ATTRIBUTES; do
  NAME=${sa%%:*}
  TYPE=${sa##*:}

  if temporal operator search-attribute list --address "$TEMPORAL_ADDRESS" --namespace "$NAMESPACE" 2>/dev/null | grep -q "^$NAME[[:space:]]"; then
    echo "Search attribute '$NAME' already registered"
    continue
  fi

  if temporal operator search-attribute create --name "$NAME" --type "$TYPE" --address "$TEMPORAL_ADDRESS" --namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "Search attribute '$NAME' ($TYPE) registered"
  else
    echo "Failed to register search attribute '$NAME' ($TYPE)"
    exit 1
  fi
done

# Nexus endpoint routing Nexus callers to the AgentOS run bridge
# (agentos/temporal/nexus_run.go). The handler is a router worker polling its
# own queue, so the endpoint targets that queue rather than a workload queue.
# Idempotent like everything above.
echo 'Creating AgentOS Nexus endpoint...'

# Names come from the same env vars the application reads, so the endpoint and
# its target queue stay in one place.
NEXUS_ENDPOINT_NAME=${AGENTFW_TEMPORAL_NEXUS_ENDPOINT:-agentos}
NEXUS_TASK_QUEUE=${AGENTFW_TEMPORAL_NEXUS_TASK_QUEUE:-agentos-nexus}

if temporal operator nexus endpoint get --name "$NEXUS_ENDPOINT_NAME" --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1; then
  echo "Nexus endpoint '$NEXUS_ENDPOINT_NAME' already exists"
elif temporal operator nexus endpoint create \
  --name "$NEXUS_ENDPOINT_NAME" \
  --target-namespace "$NAMESPACE" \
  --target-task-queue "$NEXUS_TASK_QUEUE" \
  --description "AgentOS run-control bridge. Service 'AgentOS' with operations: run (async, completes at terminal run state), run.signal, run.control, run.status." \
  --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1; then
  echo "Nexus endpoint '$NEXUS_ENDPOINT_NAME' created targeting task queue '$NEXUS_TASK_QUEUE'"
else
  echo "Failed to create Nexus endpoint '$NEXUS_ENDPOINT_NAME'"
  exit 1
fi
