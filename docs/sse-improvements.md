# SSE improvements for later review

These suggestions are recorded for later review and have not been implemented.
The functional and cost experiments use the existing SSE server. A separate
capacity experiment plans only an isolated override of connection admission
gates to identify measured limits; the optimizations below remain deferred.
The purpose of the service is immediate agent progress without committing the
tool's business transaction.

## Agent events and publishing semantics

Use one stream per agent run, spanning model requests, responses, tool calls,
and progress updates. Give events run, request, tool, and attempt identifiers so
the tracer can explain retries and overlapping work. Separate the generic event
client from the contacts-specific count and snapshot behavior in
[`client.js`](../sse_server/static/src/client.js).

Provide explicit immediate publication and publication after commit. An event
such as `tool.started` records an observation that can survive a later rollback;
`tool.committed` records a committed outcome. Returning from a tool and committing
its transaction are distinct milestones. Best-effort progress publication should
not accidentally change the business transaction's outcome when transport fails.

## Browser connections and subscriptions

Keep SharedWorker sharing between eligible tabs. For users watching several
runs, consider one worker and one SSE connection per authenticated scope carrying
multiple authorized run streams. Each event needs its stream identifier, and the
worker needs subscriptions per tab and reference counts for unused streams.

The current helper names workers by scope and stream, and the broker accepts one
stream per connection. Multiplexing therefore requires server routing and ticket
changes as well as a different worker name. Odoo should authorize every added
subscription; the relay can validate the resulting capabilities without ORM
access. Subscription changes can use HTTP control requests because EventSource
only receives events. Preserve database, user, session, and run access boundaries.

One stream per run is sufficient for the first agent experiment. Multiplexing is
a later choice if simultaneous runs justify it.

## Recovery

Recover durable run state through normal Odoo RPC. Consider a bounded recent
event buffer, event IDs, and replay after short interruptions. When history is
unavailable, send an explicit reset and refresh the durable snapshot. Runtime
progress and generated text that were never persisted may disappear after a
crash; the UI must distinguish them from committed results.

## Publishing and fanout cost

Consider encoding each event once and sharing the encoded frame across
subscribers. The current broker shares the event object but encodes the frame in
each subscriber's loop. Consider reusing publishing connections instead of
opening a Unix socket for every event. Coalesce small text deltas into bounded
chunks if measurements show that it improves cost without noticeable UI delay.

These are hypotheses to measure, not evidence that the current implementation
is slower than the bus.

## Capacity and fairness

Make connection, payload, and queue limits configurable. Retain bounded buffers,
publication deadlines, and a defined slow-consumer policy. Consider budgets and
fair scheduling across tenants and streams, especially if one connection carries
several streams or the asyncio process also handles provider networking.

The current 128 connections per database, 1,024 total connections, 100 queued
events per subscriber, and 256 KiB publishing limit are policy choices, not
benchmarked capacity figures. The
[capacity phase](sse-utility-experiment.md#third-experiment-choosing-admission-limits)
will sweep connection counts with test-only admission overrides and recommend
limits for a stated workload and resource budget. Production configurability,
new fairness mechanisms, and queue or payload policy changes remain deferred.
