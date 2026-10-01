# Reproducing the experiment

Use an isolated Odoo master (20.1) deployment and databases. Do not install the
`sse_agent_loop_experiment` addon on production. It permits administrators to
generate synthetic traffic and intentional failures, and writes two isolated
probe records per fixture. It does not implement a durable agent runtime.

The tested Python environment already contained `aiohttp`, `websocket-client`,
`psycopg2`, and `psutil`. Headless Chrome exposes DevTools on localhost port 9223.
The browser experiment uses the unmodified SSE SharedWorker and the actual Odoo
bus WebSocket protocol. The standalone page is `/sse_experiment` after login.

Initialize `odoo_sse_exp_0000` with `--with-demo -i sse_agent_loop_experiment`.
Keep HTTP, gevent and SSE ports separate (the recorded run uses 8076/8077/8078)
and proxy the browser origin on 8080. `/websocket` goes to gevent, `/sse/stream`
to SSE, and other paths to HTTP. Proxy streaming must not buffer. Configuration
and launch commands for the actual run are recorded in the results report.

```sh
SSE_BENCH_PYTHON=/home/coder/venvs/odoo/20.1/bin/python
"$SSE_BENCH_PYTHON" scripts/sse_experiment/run.py latency --repeats 2
"$SSE_BENCH_PYTHON" scripts/sse_experiment/load.py --seconds 10 --repeats 3
"$SSE_BENCH_PYTHON" scripts/sse_experiment/load.py --payloads --seconds 10 --repeats 1 --output /tmp/payloads.json
```

`run.py` rotates the delivery adapters across identical controlled delays:
20% at 50 ms, 40% at 100 ms, 30% at 200 ms, 5% at 300 ms and 5% at 500 ms.
Their weighted mean is exactly 150 ms. These are user-informed synthetic
service times, with real ORM writes and real transactions; no production tool
distribution or external model/provider performance is claimed. It additionally
runs one five-second success and one five-second failure per adapter.

The independent PostgreSQL observer checks the committed view of both probe
rows. A local audit file survives transaction rollback. Browser times measure
DOM application using millisecond wall-clock timestamps, not physical paint.
The publication ACK alone is never counted as receipt.

`load.py` schedules requests from a clock at a fixed offered rate. Each request
publishes one event through the chosen real Odoo adapter. Results include offered
and received counts, publish cost, request latency, worker PSS/FD/CPU samples,
and generator resources. The default matrix uses 1/96 subscribers, 50/200
events per second, and roughly 1 KiB payloads. Use unique streams per window to
avoid mixing retained bus history into the measurement. `--resume` only skips
completed windows; failed/incomplete windows must be described separately.

Clone an **offline** experiment template with `provision.py` to create 1,000
real databases. The template must not be in use. Provisioning and measured load
must run separately. The experiment's optional `SSE_EXPERIMENT_CRON_FLEET=1000`
hook selects only these databases for cron, avoiding other tasks' databases
and avoiding preloading all registries into the supervisor. HTTP/gevent registry
caches remain unchanged; the measured cron cache is explicitly set to one.

With 20 HTTP workers and PostgreSQL `max_connections=100`, the resource-fit run
uses `db_maxconn=3`, `db_maxconn_gevent=16`, and a soft open-file limit of 65,536.
These are recorded test settings, not universal deployment recommendations.
They change the resource budget; they do not optimize either delivery path.

For capacity testing, load the experiment addon server-wide and explicitly set
`SSE_EXPERIMENT_GLOBAL_CAP=32768` and `SSE_EXPERIMENT_TENANT_CAP=32768` in that
isolated process. The hook replaces only the two admission constants in
`Broker.stream`; production source, queues, payloads, timeouts and five-minute
ticket lifetime remain unchanged. Without these variables, the hook does
nothing. `capacity.py` signs synthetic capabilities locally; it measures relay
capacity separately from authorization throughput. Bus clients authenticate
against each active real database. Neither driver exports the signing key.

```sh
SSE_CAPACITY_PYTHON=/home/coder/playground/sse-utility-experiment/client-env/bin/python
"$SSE_CAPACITY_PYTHON" scripts/sse_experiment/capacity.py --tenants 32 --counts 256 512 1024 2048 4096
"$SSE_CAPACITY_PYTHON" scripts/sse_experiment/capacity.py --service sse --counts 8192 --seconds 900 --repeats 1 --histogram --independent-streams --output /tmp/soak.json
"$SSE_BENCH_PYTHON" scripts/sse_experiment/renewal.py --seconds 920
```

The capacity sender calls SSE's actual Unix IPC and publishes bus events through
HTTP workers. Its timestamps isolate delivery from the publication point, but
its two producer paths differ. Use `load.py` for the matched HTTP-path cost
comparison. Protocol clients renew SSE tickets immediately; `renewal.py` checks
the actual browser SharedWorker's reconnect policy separately. Same-host load
generation, small fixture schemas, and the explicitly active tenant count limit
how broadly these results can be applied.

The capacity client environment is separate and uses the pinned
`client-requirements.lock`, including `orjson` for consumer decoding. Installing
it in the Odoo environment would change the bus serialization implementation and
invalidate the controlled comparison. The Odoo environment was left unchanged.
Long-test quantiles use 0.1 ms histogram bins and event bitmaps to bound storage.
The actual matched HTTP fixture and browser measurements use the series environment.

`loop.py` hosts a fixed local fake provider on 8081, exercises three model round
trips, and fails the final tool. `occupancy.py` models zero/one/two workers held by
a synchronous ten-second wait. `observe.py` samples worker resources in a separate
process. `fault.py` pauses only the isolated 8078 listener, checks the current
publication failure policy, and resumes it in `finally`.

The [results report](../../docs/sse-utility-results.md) records the scope actually
run, including interrupted connection ramps and generator limitations.
