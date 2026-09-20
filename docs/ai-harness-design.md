# Open-source AI harness for Odoo

Status: discussion specification; no harness or runtime implementation yet.
Last updated: 2026-09-20.
Target: Odoo 20.0 Community, initially on Linux with prefork workers.

This is the working record of the architecture discussion. Read it when resuming
the project, and update it as decisions change. Preserve the distinction between
agreed direction, proposed implementation, and unresolved questions. The latest
user instructions take precedence over this record.

## 1. Objective and scope

Build an independent, open-source AI agent harness integrated with Odoo's ORM.
An agent run may alternate between model requests and tool executions, expose
live progress and text to the browser, and survive worker restarts according to
explicit recovery policies.

We do not have access to Odoo Enterprise's AI module. Earlier discussion of its
proxy/webhook architecture was background information, not verified source
evidence or an available implementation dependency. We must implement our own
harness. Integration into Enterprise AI or an existing `ai_debug` module is not
a prerequisite or the first milestone.

The intended deployment can contain approximately a thousand tenant databases
under one `odoo-bin`, with a handful of HTTP workers, a few cron workers, and a
limited continuation pool. A thousand provisioned databases and a thousand
simultaneously busy databases are different capacity requirements. No throughput
or worker-sizing claim has been benchmarked.

Develop the harness and the minimum supporting primitives together. Do not make
three completed, general-purpose infrastructure projects prerequisites to a
working agent run.

## 2. Agreed direction and constraints

| ID | Decision |
| --- | --- |
| D01 | Normal Odoo ORM execution stays synchronous. |
| D02 | A run yields at meaningful boundaries; no synchronous worker or ORM transaction remains occupied throughout model/network waits. |
| D03 | Continuation workers execute ORM steps directly, in a bounded pool separate from HTTP workers. |
| D04 | Asyncio handles model networking, local IPC, and immediate browser streaming. Its event loop starts in the child after fork. |
| D05 | The current design preference is no direct database access and no ORM/registry use in the async worker. Persistence happens on the synchronous side. |
| D06 | Reuse Odoo's existing psycopg2 stack. A second PostgreSQL driver is not justified by using asyncio alone. |
| D07 | Persist execution state and required future work. IPC messages, post-commit callbacks, and SSE buffers are not durable work queues. |
| D08 | Immediate runtime events and committed business-state events have different semantics. Never commit a business transaction merely to display progress. |
| D09 | Resources are shared across tenants and bounded globally and per tenant. Fairness is part of the initial design. |
| D10 | Start with small fixed worker populations and one async process. Elasticity and multiple async processes can follow measured need. |
| D11 | Prefer server-wide addons and a small supervisor integration over maintaining an Odoo core fork. Exact addon names remain provisional. |
| D12 | Registry performance optimization is out of scope. The user expects upstream to address it; that is a planning assumption, not a verified upstream commitment. |
| D13 | OCA queue_job is not the execution substrate: its reviewed runner dispatches jobs through an HTTP endpoint. |

The three logical primitives are a continuation executor, a model-request
dispatcher, and an SSE event service. They do not require three distinct process
types. The dispatcher and SSE service can initially share one asyncio process.

## 3. Process responsibilities

```text
Odoo master
  |
  +-- HTTP workers
  |     authenticate, authorize, start/cancel/query runs
  |
  +-- Cron workers
  |     existing scheduled application work
  |
  +-- Continuation workers: small shared pool
  |     synchronous ORM/tool execution
  |     durable run transitions and persistence handoffs
  |     release worker capacity at yield boundaries
  |
  +-- Async runtime: one process initially
        asyncio event loop
        concurrent model/API connections
        SSE browser connections and event routing
        bounded IPC and in-memory buffers
        no direct DB access or ORM business logic
```

Persistence and work discovery are synchronous responsibilities. Their exact
placement within the continuation pool or an associated bounded execution path
is unresolved. We have not decided to add a fourth worker population.

A continuation is a bounded unit of synchronous execution reconstructed from
durable records. It is not a suspended Python stack or a serialized Odoo
environment. A fresh cursor/environment is used when resuming, with the correct
tenant, user, and company context. Cursors and recordsets do not cross IPC.

Lightweight ORM tools can run inline. A long tool occupies its executing worker;
async orchestration does not make arbitrary tool code nonblocking. Separate
jobs, batching, or suitable execution lanes may be needed for heavy tools. Cron
is a possible mechanism, not a settled universal heavy-tool executor. A budget
checked between tools cannot safely preempt an arbitrary tool halfway through.

The master should supervise the added processes. A `post_load` hook is a
plausible integration point, but there is no established generic worker-type
registration API in the inspected source. Forking, signals, watchdogs, resource
limits, shutdown, and reload all need explicit integration and verification.

## 4. Request, completion, and continuation lifecycle

Proposed normal flow:

1. A short authenticated HTTP operation creates the run and durable ready work.
2. A continuation worker acquires a ready step and performs bounded ORM work.
3. When a model response is needed, it commits the outgoing request and the
   run's waiting state together, then submits the committed request over IPC.
4. The async dispatcher admits the request according to capacity and tenant
   policy, performs the network operation, and forwards live deltas to SSE.
5. The dispatcher delivers the complete result or error to the synchronous
   persistence path. Receiving a message is not yet durable acceptance.
6. The synchronous side validates request/attempt identity, persists the result
   and the required next continuation atomically, then acknowledges completion.
7. An available continuation worker resumes with a fresh environment, executes
   the returned tool calls, and eventually requests another model response or
   completes the run.

The browser connection is not the owner of execution. Losing it must not lose
the run. Cancellation is an explicit operation with a durable policy, separate
from an ordinary SSE disconnect.

Requests, tool calls, and attempts need stable identities. The exact schema is
open; likely concepts include runs, checkpoints, model requests, tool executions,
pending continuations, ownership/leases, attempts, and cancellation state.
Illustrative states such as `ready`, `waiting_model`, `waiting_tool`, `completed`,
`failed`, and `cancelled` are not yet a finalized API.

## 5. Durability and failure semantics

Persist required future work in the same tenant transaction as the state change
that requires it. A post-commit hook may provide a fast wakeup, but recovery must
not depend on the hook running successfully.

| Failure point | Required behavior or explicit unresolved policy |
| --- | --- |
| Transaction rolls back before a request is committed | No committed model request should be dispatched. |
| Process dies after commit but before IPC submission | Rediscover the pending request from durable records. |
| Submission or completion acknowledgement is lost | Correlate and deduplicate retransmissions; do not advance the run twice. |
| Continuation worker dies | Recover unfinished work using transactional ownership or leases; reject stale attempts as appropriate. |
| Async worker dies with a request in flight | Resolve the uncertain provider outcome through retrieval/idempotency support or a defined retry/failure policy. |
| Async worker dies after receiving a response but before persistence | The response can be lost. IPC acknowledgement alone does not close this window. |
| Completion is committed but the acknowledgement is lost | A repeated completion must be recognized without duplicating tools or continuations. |
| SSE connection or replay buffer is lost | Recover the UI from persisted state; transport failure must not corrupt the run. |

We do not promise exactly-once external side effects. Providers and tools may
need idempotency keys, reconciliation, or explicit handling of uncertain outcomes.
Odoo transaction retries make it especially important to separate committed
request intent from external calls.

Completion persistence needs reserved capacity or another bounded scheduling
guarantee. Long tool executions must not indefinitely prevent the runtime from
recording completed responses. If persistence falls behind, admission and memory
must remain bounded; do not accumulate unlimited unacknowledged results.

SSE alone is ephemeral. Once the same process also owns model connections, its
failure can interrupt requests as well as live updates. Durable recovery must
preserve valid run state, but does not guarantee uninterrupted generation or
preservation of every uncommitted response.

## 6. Database and asyncio boundary

The networking-only design does not require an async PostgreSQL driver. Odoo's
existing psycopg2 stack can handle persistence on the synchronous side.

The fundamental constraint is that blocking database/ORM work must not execute
on the event-loop thread. If a future measured need justifies database access
inside the async process, a bounded executor using psycopg2 remains an option.
Each operation would own its connection/cursor/transaction appropriately, with
bounded admission. Neither an executor nor a second driver has been selected.

Token and progress events do not require a database round trip. Complete request
results and meaningful run transitions require persistence at explicit boundaries.

Work discovery can use narrow queries against runtime-owned tables without
loading a registry. Business operations still use the ORM. The exact persistence
interface and whether completion acceptance requires a registry are open.

## 7. Multi-tenant scheduling and resource bounds

All worker populations serve the deployment collectively. Do not assign one
worker, persistent database connection, or preloaded registry to each tenant.
An idle tenant should primarily cost small metadata plus occasional discovery
and recovery checks.

Proposed work discovery:

- Keep durable work records in each enrolled tenant database.
- Send a shared wakeup identifying the tenant after work becomes available.
- Track active tenants and fetch bounded batches when execution capacity exists.
- Run staggered, bounded recovery scans that eventually visit every enrolled
  tenant, including tenants whose wakeup was lost.

The synchronous side performs database discovery queries. How it exchanges
ready work and capacity information with the async runtime is unresolved. Scan
frequency determines recovery latency after a lost wakeup. The normal fast path
should not scan every database for every completion.

A shared coordination database is a possible future option, not a current
requirement. Updating it and a tenant database is not automatically atomic; it
would still require a reliable handoff and repair strategy.

Minimum fairness direction:

- Rotate among tenants with runnable work; avoid one tenant filling all slots.
- Bound per-tenant and deployment-wide continuation/model-request concurrency.
- Apply provider/account rate limits as well as tenant limits.
- Bound queued work, IPC payloads, result buffers, subscriber queues, and replay
  storage. Durable backlog does not imply unlimited admission.
- Protect short persistence/control operations from starvation by long tools.
- Scope cancellation and failures so one request does not cancel unrelated
  tenants' asyncio tasks.

Registry affinity was discussed as a possible scheduling optimization. It is
not an initial requirement, and no registry profiling, cache redesign, or
upstream-performance fix is part of this project. If affinity is added later,
it must not starve tenants whose registries are cold.

Budget PostgreSQL connections across every process population. Avoid a pool
with a nonzero minimum for each of a thousand tenants. PostgreSQL connections
are database-specific; aggregate limits and bounded idle retention are needed.

Every request, result, capability, stream, and cache key must carry an appropriate
tenant identity. A numerical run ID is not globally unique across databases.
Resolve tenant access through trusted deployment/authentication information;
browser-supplied database names are not authorization. Provider credentials and
execution user/company context must not leak across tenants.

## 8. SSE and immediate events

Odoo bus notifications are transaction-bound. That is appropriate for committed
business changes, but can make a `tool.started` notification arrive only after
the tool has finished. Keep the bus for its existing application role.

Proposed synchronous publishing interface, with naming still provisional:

```python
stream.emit(stream_id, event="tool.started", data={...})
stream.emit_after_commit(env, stream_id, event="run.completed", data={...})
```

`emit` describes an observation in the running process. It may precede a rollback
or come from an attempt that is later retried. Correlation with request/tool/
attempt identity is needed to make traces understandable.

`emit_after_commit` describes committed state. It is still a best-effort stream
notification, not a durable delivery guarantee. A tool returning and its business
transaction committing are distinct events.

The browser should subscribe to an entire AI run, spanning model requests and
tool calls. User-facing progress and richer debug traces can have separate scopes
over the same transport.

Proposed authentication is a short-lived capability issued by an authenticated
Odoo request after access checks. The async service validates it without ORM
access. Signed versus opaque tokens, expiry/renewal, revocation, and storage are
not yet chosen. The design must remain compatible with a database-free service.

Each subscriber needs a bounded queue and a slow-consumer policy. Text deltas
may be coalesced into bounded snapshots when useful; aggregate generated text
also needs size limits. A slow browser must not block other subscribers, model
networking, or Odoo execution.

Possible reconnect support: bounded recent-event buffers, event IDs, and
`Last-Event-ID`. If history is unavailable, signal a reset and fetch current
durable state via normal Odoo RPC. Uncommitted streamed text may be unavailable
after a runtime crash; the UI must not present it as durably completed output.

One listener and one async process suffice for the initial topology. Multiple
async processes later require explicit internal event routing; sharing an accept
socket does not route an event to the process holding its subscriber. Broadcast
or stream-ownership routing are future options.

## 9. Webhooks and rejected assumptions

Webhooks are a transport option, not inherently unreliable and not the only
efficient way to release Odoo workers during network waits. The user's concern
is dependence on a callback arriving without adequate consumer control.

An owned dispatcher gives us direct control of admission, connections, deadlines,
and local recovery policy. It still cannot know every provider outcome after a
network/process failure. Any future webhook adapter should use the same durable
request/result semantics and offer appropriate deduplication and reconciliation.

Do not adopt OCA queue_job as the continuation executor. Its reviewed 18.0 runner
uses `/queue_job/runjob` to ask HTTP workers to execute jobs. Our continuation
workers execute steps directly in a separate pool.

## 10. Staged implementation

### Stage 1: Specify the smallest durable run

Define tenant/run/request/tool identities, yield points, ownership, atomic state
transitions, duplicate handling, cancellation, and uncertain-outcome policies.
Define the IPC handoff and acknowledgement contract. Use the fake provider to
make failures reproducible.

### Stage 2: One complete run across the minimum workers

Use a fixed continuation pool, one async dispatcher, a fake model, and a real
read-only ORM tool. Persist requests and results on the synchronous side. Prove
that another run can use continuation capacity while the first awaits a model.
Exercise restart and duplicate-delivery recovery from the beginning.

Multi-tenancy is part of this milestone: include a busy tenant, lightly active
tenants, and idle tenant entries. Increase fleet size when feasible; do not
confuse a small functional test with evidence of thousand-database capacity.

### Stage 3: Real provider and SSE

Add one provider integration and run-scoped live events. The dispatcher and SSE
service may share the async process. Verify event timing, capability isolation,
reconnect/reset behavior, slow consumers, and completion persistence under load.

### Stage 4: Expand from evidence

Add more tools, richer permission/approval flows, provider adapters, tracing, and
operational visibility. Consider elastic continuation workers, separate async
process populations, or routing changes only when measurements justify them.
Registry-performance work remains excluded unless the user changes the scope.

Initial acceptance criteria:

- Model waits release synchronous worker capacity and ORM transactions.
- `tool.started` is observable before the tool completes, without a manual
  business commit; committed completion is separately identifiable.
- Committed work survives a lost wakeup; duplicate completion does not repeat
  run advancement; interrupted attempts follow a defined recovery policy.
- The async worker performs no direct DB access or ORM business logic.
- Idle tenants do not each retain connections, workers, or async-loaded registries.
- A busy tenant cannot indefinitely starve lightly active tenants or persistence.
- Queue, connection, task, and memory limits remain effective under overload.
- Authorization and event routing preserve tenant boundaries.

## 11. Open decisions

1. Exact addon boundaries and names (`async_runtime` is provisional).
2. Prefork supervisor integration, shutdown/reload behavior, and eventual
   development support for `workers=0`.
3. Durable record schemas, lease/fencing rules, tool transaction boundaries, and
   cancellation semantics.
4. Placement and capacity reservation of synchronous discovery/persistence work.
5. IPC transport/framing, payload limits, admission, and acknowledgement protocol.
6. Tenant enrollment, discovery, wakeup mechanism, and recovery-scan schedule.
7. Capability format, tenant identity, expiration/renewal, and trace access scope.
8. Provider policy for uncertain outcomes, retries, retrieval, and cancellation.
9. SSE event schemas, replay retention, text limits, and slow-consumer behavior.
10. Concrete fairness limits, queue admission, and heavy-tool execution policy.

## 12. Source evidence and limitations

The Community source was inspected in the local Odoo 20.0 worktree at commit
`efc7cb0f13a1817a068330f14faa8dc811e31f2a` on 2026-09-20. This was static
inspection, not runtime validation or benchmarking of the proposed architecture.

| Source in the Odoo repository | Observation |
| --- | --- |
| `odoo/service/server.py`: `PreforkServer.worker_spawn`, `process_spawn`, `Worker`, `WorkerCron` | Supervision foundations exist; worker populations are explicitly integrated. Cron prioritizes notified databases and visits the remaining databases. |
| `odoo/service/server.py`: `start`, `load_server_wide_modules` | Server-wide modules load before server construction. |
| `odoo/modules/module.py`: `load_openerp_module` | The module `post_load` hook supports server-wide initialization. |
| `odoo/addons/base/models/ir_cron.py`: `_trigger`, `_acquire_one_job`, `_commit_progress` | Durable triggers, wakeups, locking, and batching exist; these are useful foundations, not a complete agent continuation API. |
| `addons/bus/models/bus.py`: `_ensure_hooks`, `_ensure_notify_hook` | Bus rows are prepared before commit and notification is sent after commit. |
| `odoo/sql_db.py`: `Cursor.commit`, `db_connect`, `ConnectionPool` | Post-commit callbacks run after database commit; the existing driver is psycopg2; connection limits are per process/pool. |
| `odoo/http/retrying.py`: `retrying` | Odoo retries selected transaction failures; external effects need explicit duplicate handling. |
| `odoo/orm/registry.py`: `Registry.__new__`, `Registry.new` | Registries are cached by database and rebuilt on a cache miss. Registry optimization is now outside our scope. |
| `addons/iap/tools/iap_tools.py`: `_iap_jsonrpc` | This outbound helper uses synchronous `requests.post`; it is not the proposed async dispatcher. |

References used in the discussion:

- [Python: developing with asyncio](https://docs.python.org/3/library/asyncio-dev.html)
- [Python: running blocking I/O in threads](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread)
- [Psycopg: thread and process safety](https://www.psycopg.org/docs/usage.html#thread-and-process-safety)
- [PostgreSQL: NOTIFY and transaction timing](https://www.postgresql.org/docs/18/sql-notify.html)
- [OCA queue_job 18.0 runner](https://raw.githubusercontent.com/OCA/queue/18.0/queue_job/jobrunner/runner.py)

## 13. Decision changes preserved from the discussion

- The starting idea was an SSE extension for existing AI instrumentation. The
  scope is now an independent Community harness because Enterprise AI is not
  available to us.
- We chose incremental complete agent runs over finishing three general-purpose
  primitives first.
- A second async PostgreSQL driver was initially suggested. The preferred design
  now keeps all DB work synchronous and outside the asyncio process.
- Registry profiling/optimization was initially suggested as a parallel concern.
  The user explicitly removed it from scope and expects upstream to address it.
- Tenant fairness remains part of the initial design, not deferred scalability
  work. Specific policies still need definition.

When continuing: preserve these decisions, resolve the smallest open contracts
needed for Stage 1, and update this document. Do not interpret this specification
as evidence that any worker, protocol, provider, or recovery path is implemented.
