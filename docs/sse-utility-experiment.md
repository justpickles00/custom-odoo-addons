# SSE utility experiment with an agent loop tracer

This is an experiment design; it contains no benchmark results. It evaluates
whether the current SSE service provides useful live agent progress while a tool
keeps its business transaction intact. It also compares that behavior with an
existing alternative: publish progress through the Odoo bus using independent
transactions. The experiment must allow either approach to perform better.

The primary question is whether the user can see model responses and tool
activity when they happen without an early business commit. Secondary questions
are publication cost and effects on ordinary Odoo traffic. A separate capacity
phase will identify appropriate admission limits for a stated workload and host
budget. A complete durable agent runtime remains outside this experiment.

## Scope and inspected implementation

The paired Odoo checkout is master, currently 20.1 development. The source
inspected for this design was Odoo `5bcf314eb0896bdf7e218341a134901004ba3d4d`
and the SSE bootstrap in addons commit
`cd6d7f5a0887c960dc00e0b9c92c28b4fbf8ff84`.

Use the existing [`publish`](../sse_server/api.py) API,
[`/sse/connect`](../sse_server/controllers.py) authorization, and
[`/sse/stream`](../sse_server/server.py) delivery. The functional and initial cost
phases keep all server limits unchanged. The capacity phase permits only an
isolated test override of the global and per-database connection gates; queue
bounds, payload size, timeouts, framing, and ticket lifetime stay unchanged.
The broker has no event replay and its publication acknowledgement means local
acceptance, not browser receipt.
Odoo's normal bus stages notifications before commit and notifies the dispatcher
after commit; its dispatch path fetches retained rows and can batch notifications.

The [improvement suggestions](sse-improvements.md) remain deferred. An experiment
fixture and consumer are needed, but they must not optimize the SSE server.
Record any capacity-only admission override as an explicit test diff and restore
the original gates for a verification run. The contacts demo is not the workload:
its staging rows, count queries, and locks obscure the transport comparison.

## Deployment target

Use the user's proposed deployment as the target: 1,000 provisioned tenant
databases served by one Odoo instance, with 20 HTTP workers, one cron worker,
and one gevent worker. These are experiment assumptions, not a claim that this
sizing is adequate or typical for every thousand-database fleet. Odoo's
[deployment guidance](https://github.com/odoo/documentation/blob/19.0/content/administration/on_premise/deploy.rst)
describes worker sizing in terms of hardware and workload, with shared HTTP
workers, additional cron workers, and an event-driven worker.

| Role | Bus deployment | Deployment with SSE | Responsibility |
| --- | --- | --- | --- |
| HTTP workers | 20 | 20 | Ordinary requests, webhook handling, ORM tools, and stream authorization |
| Cron workers | 1 | 1 | Scheduled work across the fleet |
| Gevent workers | 1 | 1 | Existing bus WebSocket connections and notification dispatch |
| SSE workers | 0 | 1 | Immediate trace delivery across all subscribed tenants |

This table excludes the supervisor, PostgreSQL, proxy, fake provider, and load
generator. HTTP workers serve requests for different databases; there is no
worker allocation per tenant. The single gevent worker includes its internal
dispatch tasks, whose count is separate from HTTP worker population. Likewise,
the current addon spawns one SSE process for the entire instance, not one per
database. Its event routing uses database and stream identity without loading
Odoo registries in that process.

For the paired master implementation, the target process settings are
`workers = 20`, `max_cron_threads = 1`, and `gevent_workers = 1`. A later
deployment with dedicated continuation workers is a different topology and
needs a separate result; the webhook experiment executes tools in HTTP workers
as proposed here.

Keep four fleet measurements distinct: provisioned databases, tenants with
ordinary application traffic, tenants with active agent runs, and connected
subscribers. One thousand provisioned databases need not produce one thousand
concurrent tools or streams. A model wait can leave a run active while no HTTP
worker is executing it. Define per-run event rate and observer fanout as well
as the number of active runs.

Use two kinds of resource comparison. For the controlled transport comparison,
keep the SSE process loaded but idle in the bus cases so process population and
resource limits stay constant. For the deployment cost comparison, measure the
bus deployment without the SSE broker against the deployment with it. Include
the added process's CPU and memory, ticket authorization and renewal requests,
and the proxy in the total. Both deployments use the same host CPU and memory
budget; an additional process is not an additional reserved core.

Capture PostgreSQL's connection limit and the process-local Odoo pool settings,
including the gevent pool override when used. These pools serve connections to
multiple databases; they do not reserve a pool per provisioned tenant. Count
actual connections across HTTP, cron, and gevent processes and other cluster
users. Separate-transaction bus progress can need extra simultaneous connections
while a tool holds its business cursor; include that demand in the comparison.
SSE authorization still uses normal HTTP workers and database access.

Warm the intended active tenant set through ordinary routing. Run a separate
profile that rotates activity into previously inactive tenants, recording
registry loading, worker memory, and request latency. Record the effective
`Registry.registries.count` and occupancy separately for HTTP, cron, and gevent
processes. Each process has its own cache; the cron setting does not determine
the HTTP cache size.

In the inspected master source, general sizing can be overridden with
`ODOO_REGISTRY_LRU_SIZE`. On Linux without that override, startup derives the
count from `limit_memory_soft` using a 15 MiB sizing heuristic per registry; the
default 2 GiB soft limit yields 136 entries. This is a count heuristic, not a
measurement of actual registry memory. Cron applies a separate
`ODOO_REGISTRY_LRU_SIZE_CRON` override when supplied; its startup hook does not
hardcode a size of one. A target cron cache of one must be confirmed from the
deployment or declared as a test assumption. See the paired Odoo source at
[`preload_registries`](https://github.com/odoo/odoo/blob/5bcf314eb0896bdf7e218341a134901004ba3d4d/odoo/service/server.py#L1482)
and
[`WorkerCron.start`](https://github.com/odoo/odoo/blob/5bcf314eb0896bdf7e218341a134901004ba3d4d/odoo/service/server.py#L1383).

Keep effective registry/cache settings identical between transport cases and
report cold activation separately from event delivery. A cache of one still
requires memory and can cause loading when the worker switches databases.
Registry optimization is outside this experiment. Keep the cron process enabled
with the same tenant list, due jobs, and schedule in every case; its polling
and registry activity are part of the fleet cost even for tenants with no user
or agent traffic.

## Deployment workload profiles

Start with the small functional fixture, then increase the real tenant fleet
through feasible stages toward 1,000 databases. The final stage must use the
target worker counts and a host budget suitable for that deployment. If this VM
cannot host the target fleet and process population, report the smaller result
and leave the target stage unexecuted; a simulated list of tenant IDs does not
establish thousand-database behavior.

The following profiles are proposed exploration points. In the target stage,
all 1,000 databases exist, while the agent workload varies independently:

| Profile | Tenants with active agent runs | Connected SSE streams | Purpose |
| --- | --- | --- | --- |
| Sparse activity | 10 | 20 | Baseline cost when most tenants have no agent traffic |
| Moderate activity | 50 | 100 | Shared workers with distributed agent traffic |
| Broad activity | 200 | 400 | Delivery across more tenants with the same process population |
| Uneven activity | 50 | 100 | One busy tenant alongside lightly active tenants |

These examples assume one run per active tenant and two observers per run.
Specify ordinary bus clients and HTTP traffic independently. Hold the offered
event rate constant in one comparison to isolate tenant distribution, and
increase it in a separate comparison to measure event-volume effects. Record
queued webhooks and active tools separately: 200 active runs do not imply that
200 tools execute concurrently through 20 HTTP workers.

Treat webhook tool execution as an ordinary UI-originated HTTP request: it uses
the same worker pool and should normally perform a relatively fast ORM operation.
Calibrate the primary workload using representative reads and writes and their
measured warm and cold latency, rather than inserting multi-second sleeps into
every tool. Increase the offered mix of ordinary UI requests and fast webhook
requests while recording worker occupancy and queueing.

Use synchronous image generation as a separate exceptional workload, simulated
by a local provider delay. Measure cases with zero, one, and two slow requests
while the remaining workers handle the fast mix. Their duration and incidence
are test parameters until deployment observations provide them. Holding all 20
workers with slow tools is an optional overload diagnostic, not the normal
agent workload. Existing SSE connections can still receive published progress,
while additional webhooks and authorization/renewal requests may queue. Measure
callback submission to handler entry, publication to receipt/render, ordinary
request latency, and renewal success separately.

The shipping global cap is 1,024 active SSE connections. One stream in each of
1,000 databases fits numerically with only 24 connections of headroom; two in
each database requires 2,000 connections and exceeds that gate. Initial utility
profiles stay below these limits. The capacity phase below will bypass these
arbitrary admission gates in an isolated test to measure sustainable operation
and propose limits. Configured fleet size, admitted connections, and sustainable
observer capacity are different measurements.

## The proposed tracer

The intended agent loop tracer addon depends on `sse_server`. It subscribes to a
run stream and shows a chronological timeline of request submission, model
response, tool execution, and committed outcomes. Emit observations at those
boundaries in the loop; the transport cannot infer what a tool is doing.

For the experiment, a provisional `sse_agent_loop_experiment` addon can depend on
`web`, `bus`, and `sse_server`. Its viewer uses the same timeline component with
three transport adapters so the bus and SSE cases have the same rendering cost.
The SSE adapter can consume the existing generic SharedWorker transport directly,
avoiding the contacts-specific page helper. The bus adapters use Odoo's current
frontend subscription APIs. None requires a production client rewrite.

Proposed event fields are `event_id`, `run_id`, `attempt_id`, `request_id`,
`tool_call_id`, `sequence`, `kind`, and a small JSON payload. The database is part
of routing and authorization. Use a run-specific stream and authorize its owner
or an experiment administrator. The synthetic response contains ordinary response
text, tool names and arguments, and explicit progress metadata.

| Event | Meaning shown by the tracer |
| --- | --- |
| `model.request_sent` | A request was handed to the fake provider |
| `model.response_received` | The webhook handler received the model response |
| `tool.started` | This tool attempt began |
| `tool.progress` | An observation during this attempt |
| `tool.returned` | The function returned; its business writes may still be uncommitted |
| `tool.committed` | The transaction containing the tool's writes committed |
| `attempt.failed` | This attempt encountered an error |
| `run.completed` | The final durable run outcome is committed |

Keep event generation, UI layout, and instrumentation identical across adapters.
Publication time, network receipt time, and DOM render time are different
measurements. Display both the event's occurrence order and its arrival time so
a delayed batch is apparent.

## Deterministic loop fixture

Use a fake provider in a separate local process. It accepts a model request,
waits two seconds, and posts a reproducible response to the experiment webhook.
The HTTP worker returns after submitting the request; it does not hold a business
transaction open throughout the model wait. This removes provider variability
while exercising a real webhook route and real Odoo transactions.

Run three model and tool rounds with fixed payloads and delays. The webhook
handler emits `model.response_received` and `tool.started`, then executes a real
ORM tool against experiment-owned probe records. The normal tool writes and
flushes the first probe without committing, writes the second, and returns
without an artificial delay. Include both a successful attempt and an injected
error after the second write that rolls back the entire tool transaction. No
external side effects are part of this fixture.

A separate ordering diagnostic inserts a five-second pause between those writes
and emits progress during the pause. It makes publication-before-commit visible
to the database observer and browser. Its delay is a diagnostic device and must
not determine the primary workload or support a claim that ordinary tools are
slow. The exceptional synchronous image case is measured separately.

Prepare and commit run identities and a separate probe pair for each tool attempt
before starting each trial. An independent database observer checks whether the
first probe's new value becomes visible while the tool is paused, and reads both
probes after the transaction ends. Use fresh observer transactions for each read.
A server-side log outside the business transaction records the actual execution
milestones and commit or rollback, so a rollback does not erase the measurement
evidence. Earlier successful attempts keep their committed results when a later
attempt fails.

Commit markers are published only after confirmed commit. In the failure case,
the driver records the HTTP outcome and rollback independently of transport;
normal bus events in the rolled-back transaction may never arrive. A failed
attempt is distinct from a final committed failure state, which can be recorded
in a subsequent normal transaction.

## Comparison cases

| Case | Progress publication | Tool transaction |
| --- | --- | --- |
| Bus in the tool transaction | Normal bus API using the tool's environment | One transaction, no manual commit |
| SSE | Current Unix publishing API | The same transaction, no manual commit |
| Bus in a separate transaction | Normal bus API in a dedicated cursor, committing progress only | The same tool transaction, no manual commit |

Build plain event payloads before entering the dedicated bus environment. Do not
reuse ORM records or uncommitted probe values through that separate cursor. It
publishes observations about an already identified run, not claims that business
writes have committed.

The separate-transaction bus case is essential. It can provide immediate UX
without an early business commit, so comparing SSE only with delayed normal bus
publication would not establish that SSE is the best implementation.

An optional fourth demonstration can commit the first probe early, then inject
the later failure. Its expected partial result illustrates why early business
commit is unacceptable for this atomic tool. It is not a valid performance
baseline and must use disposable experiment records only.

## First experiment showing live progress

Start each trial only after the subscriber is connected and ready. The bus case
must have confirmed its channel subscription; the SSE case must have received
the broker's ready message. Use a new run identity for each trial to avoid mixing
retained bus notifications or previous attempts.

For each comparison case, run 30 successes and 30 injected failures, rotating the
case order between trials. Show one representative real-browser recording per
case, with the tool's open interval and its commit or rollback marked on the
timeline. Run a smaller set of the five-second ordering diagnostics separately.
A smaller smoke run can precede the repeated trials.

The architectural behavior is already expected from the source. This experiment
checks that the complete browser, proxy, relay, and transaction integration
actually exposes it to the user.

Proposed acceptance targets for the local fixture are:

- All successful attempts preserve the intended atomic writes; all failed
  attempts leave both probes at their original values.
- In the ordering diagnostic, SSE and separate-transaction bus progress are
  rendered while the observer still sees the original probe values.
- For the five-second diagnostic, `tool.started` is visible at least four seconds
  before the tool returns in at least 95 percent of trials. This is a proposed
  diagnostic target, not a measured result or a normal tool latency assumption.
- No failure is presented as a committed success, and committed markers appear
  only after the associated transaction commits.
- Report every missing event or connection failure. Acceptance or rendering
  cannot be inferred from the publisher returning successfully.

Expect normal bus progress to arrive after a successful commit and to disappear
with a rolled-back transaction. SSE and separate-transaction bus progress should
appear during the paused diagnostic. A normal fast tool can finish before any
network event is rendered even when publication occurred before commit; measure
the actual timing and UI benefit rather than requiring a visible open interval
in every fast call. Agreement with the diagnostic expectation establishes useful
behavior, not a performance win over the immediate bus alternative.

## Second experiment measuring cost and interference

Once the functional trial passes, replay the same trace event sequence through
the three publishing adapters at a fixed offered rate. Schedule events
independently of how quickly previous publication calls return, and record any
producer backlog, rejection, or timeout. This avoids lowering the workload
silently when a publisher becomes slow.

Use approximately 1 KiB payloads for the primary profile. Start with 1, 10, 50,
and 200 logical events per second in total, and 1, 8, 32, and 96 subscribing
browser contexts in selected profiles. These are proposed exploration points;
they are not capacity claims. Hold the number of producers and tool transaction
boundaries constant between cases. Use the natural transaction intervals of the
fast tools in every mode. Do not use the five-second diagnostic to amplify SSE's
UX advantage or amortize normal bus publication cost. Measure the exceptional
slow-tool workload as a separate profile.

Begin with independent runs, one stream per browser context, distributing the
total offered event rate across runs. Add a broadcast profile where several
contexts observe one run, to measure fanout separately. Report logical events
per second and expected subscriber deliveries per second as distinct rates.
A separate modest profile can give 32 contexts three run subscriptions each.
Record the resulting socket counts: the current SSE helper can require three
connections where bus channels share one WebSocket. Compare the same observer
workload rather than forcing both implementations to have the same connection
count.

Use two-minute measured windows after warm-up, at least five repetitions, and
rotate case order. Include one separate six-minute session to exercise the
current five-minute ticket expiry and renewal behavior. Lost events during that
session are a result to report; the experiment must not add replay to hide them.

Compare ordinary bus traffic alone, ordinary bus traffic plus the custom trace
on the bus, and ordinary bus traffic plus the custom trace on SSE. Generate a
small, fixed background bus workload on a private experiment channel and a fixed
ordinary HTTP read workload. Record their latency to determine whether moving
trace events helps other users. The immediate bus alternative is the primary
performance comparison; the normal bus case documents the cost of its different
delivery timing.

For the preliminary tenant profile, use four dedicated databases with at most
96 SSE connections in each. Make one tenant busy and the others lightly active.
The deployment profiles above extend this to a provisioned fleet on suitable
hardware. Count all active experiment connections, including multiple streams,
before starting, and stay below 128 per database and 1,024 total. The preliminary
profile tests modest interference; it does not establish thousand-tenant
capacity or capacity with an admission limit removed from the server.

Include a controlled slow receiver to observe queue saturation, disconnects,
and whether healthy receivers continue receiving promptly. Bus and SSE have
different queue policies; those differences are part of the current deployment
comparison, not evidence about intrinsic WebSocket versus SSE efficiency.

## Measurements and attribution

Record event generation, publication start and return, receipt, render, tool
start and return, and commit or rollback. The bus API can return after merely
staging an event; SSE waits for a broker acknowledgement. Compare end-to-end
receipt and rendering, and report the different publication meanings alongside
call duration.

Count offered, accepted, received, and rendered events separately. For bus,
separate staged publication from committed notification rows; for SSE, record
the broker acknowledgement. Correlate events by logical IDs and subscriber
identity. Report duplicates, ordering violations, disconnects, and missing events.
Latency percentiles for successfully
received events must be presented with the delivery fraction; dropping slow
events must not make a service appear faster.

Collect CPU, peak memory, open descriptors, actual connection counts, tool and
HTTP latency, PostgreSQL connection usage, and notification rows and database
work where attributable. Account for the SSE process as well as HTTP workers,
the gevent process, PostgreSQL, and the proxy. Keep workload and host resource
budgets identical across cases, reporting both the controlled transport and
incremental deployment comparisons described above. The service improves
visibility during tool execution; it does not free an HTTP worker that is still
executing the tool.

For same-host network measurements, use a collector with the same host monotonic
clock as the publishers. Browser `performance.now()` uses a different time
origin; calibrate browser-to-driver timestamps and report uncertainty for DOM
latency. The paused tool and independent database observer also establish
progress-before-commit ordering without requiring clocks on different machines
to agree.

Record Odoo and addon commits, Python and library versions, configuration,
proxy settings, browser versions, offered load, payload distribution, and
warm-up policy. Monitor the load generator's CPU and backlog. Move it to a
separate host for capacity claims, or explicitly limit conclusions when it
shares this VM. Use database-scoped statistics and explain noise from the shared
PostgreSQL cluster; do not reset another task's statistics.

## Third experiment choosing admission limits

The current 128 per-database and 1,024 global gates are policies, not measured
capacity. This phase will propose production limits from measured behavior on
the target hardware and workload. It does not optimize the server, change its
queues, or claim that an idle-connection maximum is a useful operating limit.

Use an isolated experiment checkout with a minimal, recorded override of only
the two connection gates. Make the temporary gates high enough that they do not
bind within the requested test point, while retaining finite test ceilings and
host memory and descriptor budgets. Otherwise an HTTP 503 at connection 1,025
would merely rediscover the existing policy. Record the original and test code
revisions and diff, and distinguish admission rejection from resource failure.
No production limit changes are part of this design update.

Increase actual connected streams through proposed points such as 256, 512,
1,024, 2,048, and 4,096, stopping when the host budget or agreed service targets
fail. Refine the interval near the first failure. These are starting points for
a sweep, not predicted capacity. Measure per-database concentration separately
from total count, and test both balanced tenants and one busy tenant. Use the
same host budget for bus and SSE, and record OS and proxy limits as well as
application gates.

For each connection count, test idle streams, the normal fast-webhook workload,
bursting trace events, broadcasts, slow receivers, and reconnect/renewal churn.
Use a fixed total event rate in one series to expose connection overhead, and a
fixed per-active-run rate in another to expose growth in actual delivery work.
Include the exceptional synchronous image case in its stated frequency or
concurrency profile. Keep payload distributions, subscribed streams per user,
and fanout explicit. Count real connections after SharedWorker sharing rather
than equating tabs or provisioned databases with connections.

Define service targets before running the sweep. Proposed local targets to
review are p99 publication-to-receipt latency below 250 ms and at least 99.9
percent delivery to healthy connected subscribers, alongside a declared ordinary
HTTP and bus latency budget. Evaluate the immediate-delivery target for SSE and
the separate-transaction bus comparison; normal bus commit delay remains a
separate semantic result. These are proposed experiment criteria, not existing
product guarantees. Intentionally slow receivers have a separate disconnect and
loss result. Require memory and descriptors to remain within the assigned
budgets without unexplained growth, and record healthy-client renewal failures
and process restarts.

Use protocol clients on a capable separate load-generator host for most
connections, with real-browser canaries checking the tracer. Validate the
protocol client's subscription and renewal behavior against the browser case.
If the generator becomes the bottleneck, mark that point inconclusive rather
than assigning its failure to the server.

Repeat promising boundary points and soak the candidate operating point for
at least 15 minutes, covering multiple ticket-renewal cycles and the configured
cron workload. Extend the run or qualify the result if the relevant fleet cron
cycle is longer. Derive a global recommendation from the lowest sustainable
count across required workload profiles, then reserve headroom. Operating at
70 to 80 percent of that validated boundary is a proposed headroom policy to
review, not a universal constant.

Derive a per-tenant quota separately from the busy-tenant interference results
and the intended fairness policy. A connection-count gate alone does not bound
event rate or bytes delivered, so record those workload limits alongside the
recommended count. The result should state a capacity envelope and proposed
global/per-tenant limits for this deployment, with the assumptions and limiting
resource. It should not present one hardware-independent ideal number.

## Hypotheses and decision

These are hypotheses, not existing benchmark findings:

- Both SSE and bus with separate progress transactions can publish observations
  before business commit. They should render during the paused or slow tool;
  the visible latency advantage may be small for ordinary fast tools.
- SSE will use less database work for frequent progress because it avoids
  storing and fetching bus notification rows for those events. Count initial
  authorization and renewal requests as part of its total cost.
- Separating frequent trace traffic may improve ordinary bus latency under
  load, while shared CPU and bandwidth can still limit both services.
- The bus may be competitive for broadcasts because it batches notifications
  and serializes them once; current SSE encodes for each subscriber and opens a
  new Unix publishing connection per event.
- Short reconnects may favor bus catch-up. Current SSE can lose progress events,
  making its final durable state recovery particularly important.

Adopt the current service for the target workload if the live-progress and
atomicity checks pass, delivery and recovery behavior are acceptable, and its
cost or isolation provides a worthwhile benefit over the immediate bus
alternative. If that alternative matches the UX at lower overall cost and
complexity, the experiment should say so. If SSE misses the target under modest
load, review the deferred improvements using the measured bottleneck.

The experiment can establish value for the tested agent loop. It cannot prove
that SSE is universally faster than WebSockets or that the proposed full agent
runtime is durable and production ready.

## Planned deliverables and execution boundaries

The future implementation should produce the fixture addon, a deterministic
fake provider, a tracer page with comparison adapters, a driver and collector,
raw event measurements, representative browser recordings, and a result report
showing timing, delivery fraction, transaction outcomes, and resource cost. The
capacity phase additionally produces the admission-only test diff, sustainable
operating envelope, and recommended limits with their headroom and assumptions.

Use a separate Odoo process, database names, HTTP and gevent ports, SSE port, and
data directory for the experiment. Provision development databases with demo
data under the VM's existing conventions. Reuse the series virtual environment
when dependencies allow, and use the shared local PostgreSQL cluster. Check
listeners and task ownership before choosing ports. Keep internal services local
and use the documented Tailscale entry point if a user preview is requested.

The initial functional and cost phases require no change to an existing task
server, database, or SSE limit. The capacity phase uses its own isolated
admission override. No real provider account, external tool side effect, or
complete agent scheduler is required.
