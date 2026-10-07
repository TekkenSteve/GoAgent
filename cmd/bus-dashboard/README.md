# bus-dashboard

A live, runnable proof that the AgentOS data plane works. It generates synthetic
agent runs through the **same code paths the shipped backends use**, subscribes
to the same run channels a frontend would, and asserts the timeline invariants
the `agentos/stream` contract promises — then renders the whole thing in a
browser over Server-Sent Events.

It is the visual counterpart to the `agentos/stream` conformance suite: the
conformance tests prove the contract on a synthetic bus; this dashboard proves
it on a live one, under sustained traffic, with a human watching.

## What it verifies

| Assertion | Meaning |
| --- | --- |
| **Sequence monotonic** | the bus assigns strictly increasing sequence numbers per channel |
| **Message pairing** | every `*_MESSAGE_START` is paired with an `*_MESSAGE_END`; content deltas ride an open message |
| **Terminal closure** | each run's timeline closes exactly once with the intended terminal, nothing arrives after it |
| **Bus ↔ PG projection** (with `-pg-url`) | the milestone recorder made the same terminal milestone durable at the writer — before the bus delivered it — and the dashboard polls Postgres until the durable projection shows it |

A run fails the banner if any assertion trips; each violation carries a
human-readable message in the runs table.

## What generates the traffic

The generator publishes through two real adapter paths, never a bespoke demo
writer:

- **native** — `streamadapter.PublishWriter` (the path a native streaming
  backend uses): `RUN_STARTED`, reasoning + text token deltas, a tool-call arc
  (TOOL_CALL → `tool.execution` → TOOL_CALL_RESULT with stdout), user feedback,
  then a terminal milestone.
- **lifecycle** — `streamadapter.RunLifecycle` (the path a one-shot backend
  uses): `Start` opens the timeline, intermediate `Status` polls stay
  non-terminal, the terminal `Status` closes it.

Both paths attach the real `streamadapter.MilestoneRecorder` when `-pg-url` is
set, so every milestone is appended to Postgres at the writer — the same
fact-first path the shipped app wires — and the live subscriber observes the
timeline the facts describe.

## Run it

```bash
# in-process bus, no Postgres — the projection column shows "off"
go run ./cmd/bus-dashboard

# open http://localhost:8613
```

The page live-streams run timelines, tokens, sequence numbers, assertion
pass/fail marks, and aggregate stats. Traffic flows immediately; the "▶ Start a
run" button pushes one more run through the same real paths on demand.

### Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `-addr` | `:8613` | HTTP listen address |
| `-rate` | `25ms` | delay between token events |
| `-concurrency` | `3` | parallel synthetic run streams |
| `-runs` | `0` | stop after N runs (0 = until interrupted) |
| `-seed` | `0` | PRNG seed for reproducible traffic (0 = time-based) |
| `-mode` | `both` | publishing path: `both` \| `native` \| `lifecycle` |
| `-pg-url` | (empty) | Postgres URL enabling the writer→PG projection check |

### With the PG projection check

```bash
# start the local dev DB once
docker compose up -d db

# apply migrations (creates agentos_run_events, among others)
PG_URL='postgres://user@localhost:5432/db' \
  go run ./cmd/agentos-migrate

# run the dashboard with the recorder wired in
go run ./cmd/bus-dashboard \
  -pg-url 'postgres://user@localhost:5432/db'
```

When the transport chip shows `projection: pg` and a run's Projection column
goes `✓`, the recorder persisted the terminal milestone at the writer (before
the bus publish); a `✗` means the durable row did not appear within the
timeout.

## How it works

- **One shared in-process bus** — `memstream.New` is the Publisher *and* the
  Subscriber, exactly as the shipped app assembles them when no Centrifugo is
  configured. Publisher and subscriber must be the same instance; two instances
  would be two separate buses and nothing would reach the page.
- **Generator (write side)** — `traffic.go` draws a run identity, a path
  (`both` splits it), and a weighted terminal (clean finish dominates, with
  error and cancel still exercised), then publishes through the real adapters.
- **Observer (read side)** — `observer.go` subscribes to each run's channel
  *before* anything is published (`stream.LiveOnly`), drains it in a goroutine,
  and reduces the stream into a live view while running the three always-on
  assertions. With a repo wired it additionally polls Postgres until the run's
  terminal fact appears or the timeout elapses.
- **SSE** — every bus delivery wakes a coalescing broadcaster; the `/events`
  endpoint streams `data:` frames with dashboard snapshots. The page falls back
  to polling `/api/state` if the EventSource drops.

## Tests

```bash
go test -race ./cmd/bus-dashboard/...
```

The observer tests feed synthetic stored events to break each invariant
deterministically (terminal-after-terminal, terminal-before-start, wrong
terminal, orphan content, unclosed message, non-monotonic sequence), plus a
clean-run test that drives the real `RunLifecycle` and `PublishWriter` paths end
to end through an in-process bus.
