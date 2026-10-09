# Restaurant POS load: measured results and remaining limits

The completed baseline demonstrates **20 active restaurant databases, 80 tills,
160 open tables and eight real table actions per second for three minutes** on
this VM. Every action completed, every expected committed notification arrived,
and all persisted-order and final peer-cache checks passed. Synchronisation
commit-to-receipt p99 was **12.31 ms**. This establishes the tested baseline;
it does not establish a production tenant limit.

The strongest intervention result is at sixteen offered actions/s across the
same twenty restaurants: native refresh handling failed, while suppressing
equal same-frame refresh callbacks passed order and cache verification. That
control preserved Bus publication and delivery. It supports redundant POS
refresh work as a material cost in this configuration, which also has very
small HTTP database pools; it does not establish the cause of the reported
2,000-tenant incident.

The two attempts at 40 offered actions per second are **invalid as sustained
capacity measurements**. Both encountered HTTP errors, hit the emulator's
follower concurrency cap, accumulated work and did not reach final verification. Their
Bus delivery measurements remain useful: all recorded committed rows reached
their exact intended recipient sets. That delivery result does not make the
business workload successful.

At a matched sixteen offered actions/s, the corrected native harness failed,
while the diagnostic control that coalesces equal follower callbacks completed
all 1,440 actions and passed persisted-order/cache verification. This provides
specific evidence that redundant follower refreshes materially affect this
configuration. It does not establish a general speedup or a portable tenant
ceiling. A fifty-restaurant attempt stopped during startup on the host memory
guard, before any order workload; no 100- or 2,000-active-restaurant experiment
was attempted in round3.

These experiments use unmodified Odoo master
`5bcf314eb0896bdf7e218341a134901004ba3d4d`, real persisted POS orders, native
HTTP RPCs and authenticated WebSockets. The workload is a browser-free protocol
and cache emulator. It follows the [earlier congestion experiments](pos-bus-load-followup.md)
and [publication/read amplification audit](pos-bus-code-findings.md).

## Workload and acceptance criteria

Except for the two-till smoke, each restaurant has four independent tills and
two long-lived tables per till. Orders contain two taxed product lines. Table
actions change quantities and notes; every fourth offered action submits
preparation quantities. Each till has one serial writer and a bounded queue.
Restaurants run concurrently. Creation, initial preparation, payment and
cancellation are setup/finalization work outside the measurement window.

The paced workload offers one action per table every 20 seconds: 0.4 actions/s
per restaurant, or six actions per minute per till. The busy workload offers one
every four seconds: two actions/s per restaurant. These are controlled schedules,
not measurements of human restaurant activity. The profile does not exercise
continuous order turnover, large baskets or catalogs, OWL, IndexedDB, DOM,
kitchen screens, self-order clients, printers or payment terminals.

Acceptance requires no missed admission, no action or RPC errors, no growing
work/refresh backlog, exact delivery of all recorded committed rows, successful
persisted-order and final peer-cache verification, and no emulator cap or host
constraint invalidating attribution. The investigation's Bus latency target is
p99 below one second; it is not an Odoo SLA. A completed run and a healthy Bus
latency distribution are separate checks.

## Configuration and its constraints

All processes share one **12-vCPU, approximately 10 GB VM**, including the
client, PostgreSQL and observer. The experiment server binds to localhost and
uses fixture databases only. PostgreSQL's shared global `max_connections=100`
was unchanged.

| Setting | Value during completed runs |
| --- | --- |
| HTTP workers / cron workers | 20 / 1 |
| HTTP connection pool | `db_maxconn=2` per process |
| Gevent connection pool | `db_maxconn_gevent=40` |
| HTTP soft / hard memory limits | 1 GiB / 1.5 GiB per worker |
| Gevent soft / hard memory limits | 2 GiB / 3 GiB |
| Registry LRU | Override of 4 for smoke/four-restaurant baseline; override removed before twenty-restaurant runs |
| Native registry capacity after removal | 68, from the 1 GiB soft limit divided by 15 MiB |
| Startup concurrency | 2 devices |
| Emulator follower concurrency | 8 callbacks per till |
| Writer and follower queue capacity | 128 each per till |
| Host guard | Stops only this experiment server below 1,600 MiB available memory |

The [saved server configuration](/home/coder/playground/pos-bus-investigation/round3/odoo.conf)
and [native registry sizing](/home/coder/worktrees/justpickles00/odoo/master/odoo/service/server.py:1490)
make the registry change explicit. None of the completed measured windows showed
an Odoo HTTP/cron worker PID replacement. Minimum available host memory stayed
above the guard threshold; low `MemFree` alone is not the guard's criterion.

The HTTP pool of two is a significant constraint when each HTTP process serves
requests across twenty databases. The pool is shared across databases within a
process, rather than providing two retained connections per database. When a
matching connection is absent and the pool is full, native
[connection borrowing closes the oldest free connection before opening another](/home/coder/worktrees/justpickles00/odoo/master/odoo/sql_db.py:695).
This provides a concrete explanation for connection churn under this
configuration, although the samples do not prove how much elapsed time or CPU
it caused. These results therefore cannot be attributed to pure POS computation
or extrapolated to a differently configured HTTP/database deployment.

## Completed measurements

All counts below concern the scheduled measurement window unless explicitly
marked as final verification or whole run. Bus latency starts at the recorded
transaction commit return and ends at socket receipt. Delivery checks include
later drain receipts for commits inside the window.

| Case | Restaurants / sockets | Window | Offered / completed actions | Queue start → end | Sync p99 / maximum | Classification |
| --- | --- | --- | --- | --- | --- | --- |
| `smoke-corrected` | 1 / 2 | 30 s | 40 / 40 | 0 → 0 | 12.67 / 13.95 ms | Passed fidelity smoke |
| `restaurants-4-paced` | 4 / 16 | 180 s | 288 / 288 | 0 → 0 | 15.11 / 16.35 ms | Passed baseline |
| `restaurants-20-paced` | 20 / 80 | 180 s | 1,440 / 1,440 | 0 → 0 | 12.31 / 19.92 ms | Passed baseline |
| `restaurants-20-busy-native` | 20 / 80 | 120 s | 4,800 / 1,813 | 0 → 2,817 | 311.26 / 641.67 ms | Invalid sustained-capacity case |
| `restaurants-20-busy-coalesced` | 20 / 80 | 120 s | 4,800 / 2,559 | 0 → 2,067 | 205.47 / 458.85 ms | Invalid sustained-capacity control |
| `restaurants-20-medium-native` | 20 / 80 | 90 s | 1,440 / 1,214 | 0 → 99 | 210.80 / 371.07 ms | Failed under corrected fail-stop harness |
| `restaurants-20-medium-coalesced` | 20 / 80 | 90 s | 1,440 / 1,439 (+1 during drain) | 0 → 1 | 12.53 / 47.56 ms | Passed diagnostic control; peak action backlog 2 |

No bounded writer queue dropped a scheduled action in the five original windows.
That means admission into the queue, not successful execution. The busy native
case started 2,063 actions and the busy control 2,813; their completion rates
were 15.11/s and 21.33/s against an offered 40/s.

| Case | Committed sync rows | Exact sync deliveries received / expected | State deliveries received / expected | Final verified orders / peer-cache assertions |
| --- | --- | --- | --- | --- |
| Smoke | 80 | 160 / 160 | 80 / 80 | 4 / 8 |
| Four paced | 576 | 2,304 / 2,304 | 1,152 / 1,152 | 32 / 128 |
| Twenty paced | 2,880 | 11,520 / 11,520 | 5,760 / 5,760 | 160 / 640 |
| Busy native | 3,758 | 15,032 / 15,032 | 7,516 / 7,516 | Missing; run failed |
| Busy coalesced | 5,332 | 21,328 / 21,328 | 10,664 / 10,664 | Missing; run failed |
| Medium native | 2,428 | 9,712 / 9,712 | 4,856 / 4,856 | Missing; run failed |
| Medium coalesced | 2,878 | 11,512 / 11,512 | 5,756 / 5,756 | 160 / 640 |

The smoke finalized two paid and two cancelled orders. The four-restaurant
baseline finalized 24 paid and eight cancelled. The twenty-restaurant baseline
finalized 120 paid and forty cancelled. Independent SQL verification checked
orders, line amounts, taxes, payments and the preparation graph. No measured
`load_data` cold relation RPC, follower saturation or workload error occurred in
those three passing windows. No recorded probe drops were reported.

### HTTP, memory and sampled process evidence

CPU percentages below aggregate a role's processes and use one CPU core as
100%; 614% represents roughly 6.14 cores. Odoo's non-gevent child command lines
do not distinguish HTTP workers from cron, so their values are combined.
Including the master raises the twenty-paced value from 223.7% to about 225.4%,
and busy native from 613.9% to about 615.5%.

| Case | HTTP/cron mean CPU | Gevent mean CPU | Client mean CPU | Minimum host available memory | Sampled unique PostgreSQL PIDs / observed appearances |
| --- | --- | --- | --- | --- | --- |
| Smoke | 28.2% | 0.42% | 1.6% | 6.45 GiB | 40 / 4 |
| Four paced | 53.6% | 0.59% | 2.9% | 5.61 GiB | 1,054 / 990 |
| Twenty paced | 223.7% | 1.99% | 10.0% | 2.42 GiB | 4,856 / 4,770 |
| Busy native | 613.9% | 6.14% | 34.3% | 2.29 GiB | 3,904 / 3,816 |
| Busy coalesced | 574.9% | 8.55% | 34.1% | 2.21 GiB | 3,576 / 3,488 |
| Medium coalesced | 280.9% | 4.35% | 15.6% | 2.22 GiB | 2,678 / 2,594 |

**PostgreSQL PID/churn counts are lower bounds from approximately one-second
samples**, not connection-creation totals. A backend can start and exit entirely
between samples. CPU deltas require the same PID in adjacent samples, so brief
backends and their connection/setup work are omitted. The recorded PostgreSQL
CPU averages, around 6–8% in these cases, cannot establish that PostgreSQL or
connection churn was inexpensive. The observer includes other VM PostgreSQL
activity, and sums of PostgreSQL RSS include shared pages rather than independent
physical memory. Peak sampled `pg_stat_activity` rows including background
workers were 89 for twenty paced, 92 for busy native and 91 for busy coalesced;
these sampled counts do not rule out short peaks at the global limit.

Twenty paced completed 8,640 follower `read_config_open_orders` RPCs in its
measurement window, averaging 63.14 ms, p99 87.92 ms. Its 1,440 `sync_from_ui`
RPCs averaged 69.32 ms, p99 108.13 ms. In busy native, successful follower RPCs
averaged 2.90 s, p99 13.83 s; successful sync RPCs averaged 2.42 s, p99 10.23 s.
Busy coalesced follower RPCs averaged 3.52 s, p99 16.38 s. These distributions
exclude failed/inflight requests and cannot be treated as complete request
latency distributions.

The client's load-phase loop-lag histogram had p99 upper bounds of 2 ms, 4 ms
and 4 ms in the three passing cases. Both busy cases reached a 128 ms p99 upper
bound, with maxima of 753 ms native and 526 ms coalesced. Receipt latency in
those cases includes client scheduling delay. Gevent remained a small part of
measured CPU; these results do not demonstrate a saturated Bus dispatcher.

## Why the busy pair is invalid, and what its control shows

During the busy native window, 912 RPC errors, 803 follower errors, 170 table
action errors and 8,626 follower saturation events occurred. The coalesced
window recorded 579 RPC errors, 512 follower errors, 174 table action errors
and 339 saturation events. Across the entire runs, native recorded 2,458 RPC
errors and coalesced 1,011. Whole-run errors included HTTP 408 responses,
connection resets, empty timeout messages and server assertions. HTTP error
status does not establish whether the associated transaction committed.

The old harness marked a till failed but continued scheduling and mutating its
orders. It lacked native offline dirty-state retry/reconciliation. Following an
unacknowledged mutation, local dirty fields and totals could diverge from the
server. The
[read-only native-run reconciliation](/home/coder/playground/pos-bus-investigation/round3/results/restaurants-20-busy-native/failed-order-reconciliation.json)
found inconsistent order-header versus line totals on **seven of 160 orders**
before cleanup. This is evidence that the subsequent workload state was invalid;
it is not evidence of an Odoo accounting defect. Neither busy case reached the
normal final persisted-order/peer-cache acceptance checks. Native also timed out
its work drain and abandoned 501 queued actions.

The harness now
[stops further mutations on a failed till](/home/coder/worktrees/justpickles00/custom-odoo-addons/master-pos-bus-investigation/scripts/pos_bus/restaurant_load.py:205)
and records queued/future actions as abandoned. An ambiguous failed action still
requires independent reconciliation. This prevents continuation from silently
invalidating future experiments; it does not implement native offline behavior
or retroactively validate the busy pair.

The diagnostic `--coalesce-equal-sync` control changes only the emulator's
handling of equal synchronization callbacks in the same received frame. Native
server publication, Bus rows and recipient delivery remain intact. In the
coalesced busy window, **10,660 callbacks were suppressed**, but only **7,995**
were same-session notifications from another sender that would issue an
open-order read. The remaining **2,665** were sender callbacks that native skip
rules already discard; they are not saved HTTP requests. Attribution joins
`coalesced_follower_callback` to notification `(device,bus_id)`, sender identity
and session evidence.

The native twenty-paced window had 4,320 equal-frame, other-sender callbacks
that could qualify for this diagnostic suppression; it actually executed all
8,640 follower reads. Source and measured publication counts establish the
read amplification. The failed busy pair suggests that suppressing redundant
callbacks can reduce offered follower work, but it is one sequential pair with
different committed work after errors. Its completion-rate or latency difference
is not a valid causal capacity/speedup estimate.

### Corrected medium native/control pair

The corrected fail-stop harness offered sixteen actions/s for ninety seconds.
It completed 1,214 of 1,440 actions inside the window (13.49/s), with twelve
measured action errors and an end backlog of 99. Its 1,078 measured follower
saturation events invalidate a claim of uncapped native follower behavior.
Across the whole run, sixteen writer errors stopped their tills, 140 queued or
future actions were abandoned, and 341 follower errors occurred. There was no
normal final persisted-order/peer-cache verification. Future scheduled actions
abandoned by fail-stop must not be counted as queue admissions merely because
the bounded queue did not overflow.

All recorded measured sync and state deliveries nevertheless matched their
recipient sets, with sync p99 210.80 ms. HTTP/cron CPU averaged 579.8% and gevent
5.28%; these remain combined workload/configuration observations with the pool
and PostgreSQL sampling limitations above. No new host/session counters existed
for this native run, so later counter measurements must not be backfilled into
it. The
[medium-native cleanup](/home/coder/playground/pos-bus-investigation/round3/results/cleanup-medium-native-applied/summary.json)
subsequently cancelled all 160 owned orders through native APIs and verified
that cleanup separately from workload acceptance.

The matching coalesced control completed all 1,440 scheduled actions, with
1,439 inside the ninety-second window and one inflight action finishing during
drain. Its action backlog peaked at two, ended at one, and follower backlog
peaked at six. There were no RPC/action/follower errors, saturation events,
admission misses or measured cold `load_data` RPCs. All 160 orders passed SQL
verification (120 paid, forty cancelled), all 640 final cache assertions passed,
and every recorded committed notification reached its exact recipient set.
The one end-boundary inflight action is disclosed rather than described as zero
backlog or a perfectly exact sixteen completions/s inside the window.

The control suppressed 4,317 eligible other-sender open-order refresh requests
inside the window and separately skipped 1,439 redundant sender callbacks that
would not have issued HTTP reads. Its 4,314 successful follower reads completed
inside the window at mean 69.01 ms, p99 99.90 ms; receipt/callback and RPC
completion windows differ slightly at their edges. Sync commit-to-receipt p99
was 12.53 ms. This successful control versus the failed native sixteen-action/s
case provides specific support for redundant refresh work being material under
these settings. It remains one sequential pair with different work after the
native errors, rather than a repeated randomized/ABBA comparison or a universal
speedup estimate. It establishes control behavior, not native capacity at
sixteen actions/s.

For this control only, additional
[host and PostgreSQL cumulative counters](/home/coder/playground/pos-bus-investigation/round3/results/restaurants-20-medium-coalesced/host-analysis.json)
cover an 89.13-second sample span. Mean whole-host non-idle CPU was 45.78% of the
twelve-core host, including 0.26% iowait and 0.07% steal. Cumulative database
session deltas show **47.09 new tenant-database sessions/s**, plus **16.66 new
system-database sessions/s**. This quantifies substantial connection creation
that a PID-only observer can miss and strengthens the pool-churn caveat. These
are shared-host/database counters with statistics-publication lag; the system
database includes other clients. They do not isolate the POS application's CPU
or prove that every new session resulted from its HTTP pool. The observer began
late in round3; no equivalent whole-host/session-delta comparison exists for the
native case or earlier runs.

## Source facts relevant to scaling

For the installed restaurant/self-order module combination, ordinary successful
single-order sync publishes two equal invalidations with distinct Bus IDs and
one state notification. The base
[`sync_from_ui`](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/models/pos_order.py:924)
publishes once; the
[self-order override and helper](/home/coder/worktrees/justpickles00/odoo/master/addons/pos_self_order/models/pos_order.py:34)
publish again, even with self ordering disabled. The
[empty-record notification path](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/models/pos_config.py:256)
produces small invalidation payloads, rather than large serialized baskets.
These fixtures' synchronization payloads are 73–74 bytes and state payloads two
bytes, excluding the envelope. Every ordinary measured sync action in the
passing windows produced two sync rows and one state row. Native preparation
checks can also complete without a synchronization RPC; finalization recorded
two, 24 and 120 such noops in the three passing cases.

The
[native follower skip/refresh path](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/static/src/app/utils/devices_synchronisation.js:64)
ignores the sender or another session, and otherwise reads open orders per
notification. With four tills, two publications imply six eligible follower
reads per originating action. At eight actions/s, the paced result is therefore
48 follower reads/s, matching the measured 8,640 in 180 seconds.

Bus has a separate gevent pool and
[derives dispatch worker count](/home/coder/worktrees/justpickles00/odoo/master/addons/bus/bus_dispatcher.py:292)
from it. With forty connections and the default 30% reserve, this is 27 dispatch
workers after reserving the listener connection. Increasing sockets or tenant
count is not equivalent to saturating those workers; database activity, channel
activity, HTTP work and connection residency matter.

Channel history is
[kept per subscribed topic](/home/coder/worktrees/justpickles00/odoo/master/addons/bus/bus_dispatcher.py:119)
for a ten-second safety interval to handle insertion IDs committing out of
order. The earlier heavy synthetic experiment concentrated roughly
33,000–34,000 rows/s across twenty databases, producing high per-channel rates.
The real restaurant workloads here use many comparatively light channels.
The synthetic result did not prove that ordinary restaurant traffic creates
long history lists on individual channels. At the paced rate, each restaurant
configuration channel receives about 1.2 POS rows/s, or roughly twelve new IDs
in ten seconds before other traffic. Topic count and history-list length are
separate scaling dimensions.

The user-reported experience of roughly 2,000 production tenants lacks known
hardware, worker/pool settings, active tenant mix, module configuration, traffic
and latency criteria. These experiments neither reproduce nor contradict that
experience. The measured claim here is the explicit twenty-active-restaurant
baseline on this constrained shared VM; neither the anecdote nor the failed
busy pair supports a tenant-count ceiling.

## Fidelity, exclusions and cleanup

Before measurement, the emulator's preparation path was corrected to read order
`write_date`, send quantity deltas, and avoid unnecessary sync for clean
preparation. Cancellation first cancels prepared quantities. Payment submits
outstanding kitchen changes before payment and performs the native post-payment
preparation check. Upstream Odoo was not patched.

The first attempted smoke used a table containing a retained round2 draft.
Native creation merged into order 73 in `posbus_0000`, and the emulator rejected
the unexpected UUID after that setup mutation. It has no measurement window
and is excluded. A saved occupancy preflight now rejects occupied intended
tables before registering devices or creating orders. Subsequent completed
runs used `posbus_0004` onward, leaving 0000–0003 out of the workload. Original
round2 raw evidence remains unchanged; the attempted smoke's mutation must not
be confused with that earlier captured state.

After each failed busy case, task-owned order identities and authoritative
native snapshots were retained. The
[native dry-run cleanup](/home/coder/playground/pos-bus-investigation/round3/results/cleanup-busy-native-dry/summary.json)
identified 160 draft orders. The
[native applied cleanup](/home/coder/playground/pos-bus-investigation/round3/results/cleanup-busy-native-applied/summary.json)
and
[coalesced applied cleanup](/home/coder/playground/pos-bus-investigation/round3/results/cleanup-busy-coalesced-applied/summary.json)
each report 160 cancellations through native APIs, with `passed=true`.
Each directory retains `before.json`, the ownership/request record and RPC log;
cleanup does not replace or repair the experiment's original result.

## Fifty-restaurant startup interruption

The
[fifty-restaurant attempt](/home/coder/playground/pos-bus-investigation/round3/results/restaurants-50-paced/summary.json)
completed all fifty table-occupancy preflights and started 116 of the intended
200 till devices. It ended after 62.79 seconds during startup. It created no
orders, emitted no `table_action_start` and never reached `measurement_start`.
The
[memory guard record](/home/coder/playground/pos-bus-investigation/round3/guard.log)
records stopping only experiment parent PID 284016 at
`wall_ns=1791552122344498926`, with `MemAvailable=1,609,364 KiB` (1,571.6 MiB),
below its 1,600 MiB threshold. The 116 socket disconnects followed the deliberate
server stop.

This is a startup memory-headroom interruption on this shared VM, not a Bus
transport-capacity failure or a successful fifty-tenant workload. No larger
100/2,000-tenant attempt is supported by this run. The experiment server and its
observers were stopped after evidence capture; the shared PostgreSQL service
was not reconfigured or stopped.

## Evidence and reproducibility

The runtime evidence root is
[round3](/home/coder/playground/pos-bus-investigation/round3).
Each completed case under `results/` retains `clients.jsonl`, `summary.json`,
`analysis.json`, `resource-analysis.json`, `restaurant-report.json` and owned
order identities; successful cases additionally have expected orders and
persisted verification. Shared `probe/probe-*.jsonl` records committed Bus IDs,
metadata and dispatcher spans. `telemetry.jsonl`, server logs/configuration,
guard records and cleanup snapshots establish the operating context.

The [checked-in evidence index](pos-bus-results-round3/README.md) records
compressed raw archives and their verified SHA-256 manifest. These artifacts
are saved on the investigation branch alongside the scripts and report. The
[read-only report script](/home/coder/worktrees/justpickles00/custom-odoo-addons/master-pos-bus-investigation/scripts/pos_bus/restaurant_report.py)
reconstructs action, delivery, callback, resource and audit summaries without
RPCs or database access; `--include-actions` adds operation-level attribution.

Validation: seven probe semantic checks passed, Python compilation passed for
the experiment scripts, and the raw archives were decompressed and verified
against their source-file SHA-256 hashes. No upstream Odoo change was made.
