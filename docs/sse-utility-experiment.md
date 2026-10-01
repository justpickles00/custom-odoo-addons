# SSE utility experiment with an agent loop tracer

This is an experiment design; it contains no benchmark results. It evaluates
whether the current SSE service provides useful live agent progress while a tool
keeps its business transaction intact. It also compares that behavior with an
existing alternative: publish progress through the Odoo bus using independent
transactions. The experiment must allow either approach to perform better.

The primary question is whether the user can see model responses and tool
activity when they happen without an early business commit. Secondary questions
are the publication cost, effect on ordinary Odoo traffic, and behavior under
modest load. Maximum connection capacity and a complete durable agent runtime
are outside this first experiment.

## Scope and inspected implementation

The paired Odoo checkout is master, currently 20.1 development. The source
inspected for this design was Odoo `5bcf314eb0896bdf7e218341a134901004ba3d4d`
and the SSE bootstrap in addons commit
`cd6d7f5a0887c960dc00e0b9c92c28b4fbf8ff84`.

Use the existing [`publish`](../sse_server/api.py) API,
[`/sse/connect`](../sse_server/controllers.py) authorization, and
[`/sse/stream`](../sse_server/server.py) delivery. Keep all server limits,
timeouts, framing, and ticket lifetime unchanged. The broker has no event replay
and its publication acknowledgement means local acceptance, not browser receipt.
Odoo's normal bus stages notifications before commit and notifies the dispatcher
after commit; its dispatch path fetches retained rows and can batch notifications.

The [improvement suggestions](sse-improvements.md) remain deferred. An experiment
fixture and consumer are needed, but they must not optimize or alter the SSE
server. The contacts demo is not the workload: its staging rows, count queries,
and locks would obscure the transport comparison.

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
ORM tool against experiment-owned probe records. The slow tool writes and flushes
the first probe without committing, remains open for five seconds while emitting
progress, then writes the second probe. Include both a successful attempt and an
injected error after the second write that rolls back the entire tool
transaction. No external side effects are part of this fixture.

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
timeline. A smaller smoke run can precede the repeated trials.

The architectural behavior is already expected from the source. This experiment
checks that the complete browser, proxy, relay, and transaction integration
actually exposes it to the user.

Proposed acceptance targets for the local fixture are:

- All successful attempts preserve the intended atomic writes; all failed
  attempts leave both probes at their original values.
- SSE and separate-transaction bus progress are rendered while the observer
  still sees the original probe values.
- For the five-second tool, `tool.started` is visible at least four seconds
  before the tool returns in at least 95 percent of trials. This is a proposed
  UX target, not a measured result.
- No failure is presented as a committed success, and committed markers appear
  only after the associated transaction commits.
- Report every missing event or connection failure. Acceptance or rendering
  cannot be inferred from the publisher returning successfully.

Expect normal bus progress to arrive after a successful commit and to disappear
with a rolled-back transaction. Expect SSE and separate-transaction bus progress
to appear during execution. Agreement with this expectation establishes useful
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
boundaries constant between cases. In the normal bus case, preserve the same
five-second transaction intervals rather than introducing per-event commits to
improve its latency.

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

For a small tenant profile, use four dedicated databases with at most 96 SSE
connections in each. Make one tenant busy and the others lightly active. Count
all active experiment connections, including multiple streams, before starting.
Stay below 128 per database and 1,024 total. This tests modest interference,
not thousand-tenant capacity or an admission limit removed from the server.

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
the gevent process, PostgreSQL, and the proxy. Keep deployment settings and
resource budgets identical across cases. The service improves visibility during
tool execution; it does not free an HTTP worker that is still executing the tool.

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

## Hypotheses and decision

These are hypotheses, not existing benchmark findings:

- Both SSE and bus with separate progress transactions will provide prompt
  observations while the business transaction remains open.
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
showing timing, delivery fraction, transaction outcomes, and resource cost.

Use a separate Odoo process, database names, HTTP and gevent ports, SSE port, and
data directory for the experiment. Provision development databases with demo
data under the VM's existing conventions. Reuse the series virtual environment
when dependencies allow, and use the shared local PostgreSQL cluster. Check
listeners and task ownership before choosing ports. Keep internal services local
and use the documented Tailscale entry point if a user preview is requested.

No existing task server, database, or SSE limit needs to change to implement
this first experiment. No real provider account, external tool side effect, or
complete agent scheduler is required.
