# Startup failures before the POS Bus baseline

Two isolated experiment startup attempts failed before a steady order workload.
They establish limits of those startup configurations on this VM, not a Bus
throughput limit or a production sizing recommendation. Source references below
use Odoo master `5bcf314eb0896bdf7e218341a134901004ba3d4d`; dates are UTC,
2026-10-09. No other Odoo deployment was active during these attempts, and no
other deployment was stopped or reconfigured.

## 100-database eager preload: memory exhaustion

The first fleet server preloaded `posbus_0000` through `posbus_0099` before
starting 20 prefork HTTP workers, one gevent process, and one cron worker.
It used `ODOO_REGISTRY_LRU_SIZE=8` and a 512 MiB per-worker soft memory limit.
The LRU bound limited retained registry entries; it did not make the allocation
and residency effects of loading 100 registries disappear before forking.

Evidence in [the server log](/home/coder/playground/pos-bus-investigation/odoo.log):

- Parent PID 182807 loaded the final databases, including `posbus_0099`, at
  approximately 10:27:21.
- Workers reported `Virtual memory limit (737013760) reached` immediately after
  starting, and subsequently exited with `request_count: 0, registry count: 8`.
  The number in that message is measured virtual memory, not the configured
  limit. The workers inherited a parent address space already above the soft
  limit, so recycling could not make the forked starting state small enough.
- The gevent process independently began preloading the same long database list;
  its registry load times rose from roughly 0.15 seconds to over 1 second as
  memory pressure grew.

The independent [resource samples](/home/coder/playground/pos-bus-investigation/results/resources.jsonl)
show `MemAvailable = 401432 kB` at 10:27:36.833837, with 23 experiment processes.
At that sample the parent RSS was 558,379,008 bytes. These are host availability
and one process's RSS; summing all forked RSS would double-count shared pages.

The kernel journal confirms a global OOM at 10:27:37, killing experiment parent
PID 182807: `Out of memory: Killed process 182807 (python)` with
`total-vm:711528kB` and `anon-rss:539784kB`. The journal also reports no swap.
Reproduce the read-only evidence query with:

```sh
journalctl -k --since '2026-10-09 10:27:35 UTC' --until '2026-10-09 10:27:39 UTC' --no-pager
```

The source explains why this was a startup memory problem. The multiprocess
server [preloads before spawning workers](/home/coder/worktrees/justpickles00/odoo/master/odoo/service/server.py:1108).
`preload_registries` [sets the LRU bound and still loops through every supplied database](/home/coder/worktrees/justpickles00/odoo/master/odoo/service/server.py:1490).
Worker [soft-limit checking](/home/coder/worktrees/justpickles00/odoo/master/odoo/service/server.py:1200)
runs before processing requests and marks an over-limit worker for exit.
The logs prove zero requests in the recycled HTTP workers; this attempt cannot
measure notification fanout capacity.

## 20-tenant cold startup: gevent connection pool exhaustion

The next server preloaded only `posbus_0000`; HTTP requests for the other fixture
databases loaded registries on demand. It retained 20 prefork HTTP workers,
one gevent process, and one idle cron worker. Settings were:

- `ODOO_REGISTRY_LRU_SIZE=2`, cron LRU size 1.
- `db_maxconn=3` per HTTP process and `db_maxconn_gevent=16`.
- HTTP worker soft/hard memory limits increased to 2 GiB / 2.5 GiB.
- Load-generator startup concurrency 12, targeting 20 tenants: four tills in
  each of four active restaurant tenants and two background subscribers in
  every tenant, for 56 intended sockets.

The failed run's [summary](/home/coder/playground/pos-bus-investigation/results/fleet-20-native/summary.json)
and [client events](/home/coder/playground/pos-bus-investigation/results/fleet-20-native/clients.jsonl)
identify run `8894f461e44d`. It ended after 10.54 seconds, with 29 devices started,
one run error, and no order lifecycle or notification receipt events. The first
run error was a websocket handshake receiving HTTP 500. The later
`Connector is closed` RPC error occurred during cancellation after that failure;
it is not the initiating cause.

At 10:30:43.703, gevent PID 184178 returned HTTP 500 for `posbus_0009`'s
`GET /websocket`: native `serve_db` called `registry.cursor`, and the local
connection pool raised `PoolError('The Connection Pool Is Full')`. At
10:30:43.708 a `posbus_0008` handshake failed through transaction rollback,
registry reset/reload, and the same pool error. At 10:30:44.643 a Bus dispatch
worker also hit this pool error during the cold session-query setup path:
`_kick_invalid_sessions -> check_sessions -> _get_session_token_query_params
-> Environment -> Registry.new -> cursor`.

This is direct evidence of a shared gevent pool becoming full during startup.
The server log also shows repeated registry reloads across tenants, typically
0.15–0.25 seconds. With only two cached registries, concurrent cold requests
can evict each other's registries, and registry setup itself needs connections.
The evidence supports registry churn and cold initialization as contributors;
it does not quantify each contributor separately.

Relevant native source paths:

- [HTTP handshake preparation](/home/coder/worktrees/justpickles00/odoo/master/odoo/http/router.py:385)
  obtains a registry and cursor before upgrading to a websocket.
- [Pool borrowing](/home/coder/worktrees/justpickles00/odoo/master/odoo/sql_db.py:695)
  raises immediately when the bounded pool is full and no idle connection can
  be replaced. This is Odoo's local pool limit, not proof that PostgreSQL's
  server-wide connection limit was reached.
- [Dispatcher worker sizing](/home/coder/worktrees/justpickles00/odoo/master/addons/bus/bus_dispatcher.py:292)
  reserves `ceil(16 × 0.3) = 5` cursors plus one listener cursor, yielding ten
  dispatch workers. That reserve is not a separate partition that can guarantee
  every concurrent handshake and nested registry initialization a connection.
- [Cold session query setup](/home/coder/worktrees/justpickles00/odoo/master/addons/bus/session_helpers.py:35)
  caches query shape by tenant but constructs an environment on its initial
  cache miss. Subsequent warm checks normally avoid this registry construction.
- [Registry lookup](/home/coder/worktrees/justpickles00/odoo/master/odoo/orm/registry.py:105)
  rebuilds a missing registry under the registry lock.
- [Transaction reset](/home/coder/worktrees/justpickles00/odoo/master/odoo/orm/environments.py:892)
  looks up the registry again after commit/rollback; commit
  [invokes that reset](/home/coder/worktrees/justpickles00/odoo/master/odoo/orm/environments.py:975).
  Eviction can therefore cause registry work during cursor cleanup as well as
  at the initial request or session check.

The startup interval contained 23 completed probe dispatch batches. Their wall
times had median 26.46 ms, p95 1915.01 ms; session validation had median 9.79 ms,
p95 776.50 ms; fetching had p95 16.04 ms; topic queue wait had p95 7.09 ms.
The three slowest batches fetched zero rows, and no serialization/enqueue work
was recorded. The large remaining batch time includes cursor context exit and
scheduler delays; the current spans do not isolate registry reload during that
exit. These startup spans are not a native-notification latency baseline and
cannot attribute the failure to payload serialization, enqueueing, or
postcommit NOTIFY.

## Controls for the next attempt

`fleet-20-paced` keeps the same server setup and reduces startup concurrency to
one. Its outcome belongs in the final experiment report; this document does not
assume success or extrapolate steady-state capacity from a startup result.
The template's 24 initially active cron jobs were disabled before cloning, and
the server's explicit database list limits cron scanning to the isolated idle
fixture database. Ordinary HTTP access to the other fixture databases is still
possible through their authenticated sessions and the experiment dbfilter.

The experiment now has a [memory guard](/home/coder/worktrees/justpickles00/custom-odoo-addons/master-pos-bus-investigation/scripts/pos_bus/guard.py): every
0.5 seconds it checks host `MemAvailable`, and below 1200 MiB sends SIGTERM only
to the specified experiment parent after verifying its exact experiment config
path in `/proc/PID/cmdline`. It neither stops other deployments nor changes the
PostgreSQL cluster. The guard was added after the eager-preload OOM; it is a
protective control, not evidence that the earlier memory conditions were safe.
