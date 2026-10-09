# Follow-up evidence

Read the [report](../pos-bus-load-followup.md) for interpretation and limits.
All runs use isolated synthetic databases and zero browsers.

| Evidence | Meaning |
| --- | --- |
| `results/cohorts-40k-02/aggregate.json` | Four-process delivery totals, admission misses and backlog, dispatcher tracing enabled |
| `results/cohorts-40k-02/server-delivery-analysis.json` | Exact join of every committed row to native server dispatch; independent of client timing |
| `results/cohorts-40k-stages-off/aggregate.json` | Same offered load with dispatcher tracing disabled |
| Both cohort directories' per-cohort summaries/resource analyses | Achieved rates, transactions, receipt counts, sampled CPU and event-loop limits |
| `results/ramp20-batched` | Initial single-process publisher limitation and uneven overload admission |
| `results/ramp20-larger-batches` | Higher achieved row rate, but one receiver/producer becomes limiting |
| `results/restaurant-20-sustained-02/analysis.json` | Use `measurement_stream_delivery` and `measurement_commit_to_receipt_ms` for the 180-second window; later drain ended unsuccessfully |
| `results/restaurant-20-sustained-02/persisted-interrupted-audit.json` | Read-only reconciliation of 32 real draft orders; no claim of completed finalization |
| `results/restaurant-smoke-02/persisted-verification.json` | Successful four-order payment/cancellation/preparation smoke |
| `results/query-history.json` | Read-only identical-row query diagnostic with 1k/10k/30k exclusion IDs |

The native 40k offered-rate runs exit nonzero because some offered work was
rejected before publication. Every committed row nevertheless reached all four
intended recipients. Do not convert those exits to a 40k/s passing capacity test.

Failed startup/setup cases are retained: `startup-64` hit PostgreSQL's global
connection limit; `cohorts-40k` lost its listener during gevent memory recycling;
the first restaurant setup hit order-reference sequence contention. The initial
smokes also exposed harness cursor-budget and cancel-response bugs. These cases
are excluded from successful transport/correctness totals. The first two ramp
scripts admitted overloaded tenants in ascending order; later code rotates
admission. The sustained run's negative live interval deltas are a corrected
display-counter bug; raw timestamps and queue samples support its reported rates.

Five raw archives preserve the client streams, producer commits, server probes,
resource samples, logs, control gates and complete arguments. Extract all into
one scratch directory to reconstruct the runtime layout. `manifest.json` gives
archive SHA-256 values, sizes and verified decompressed file counts. Every
archived file was checked against its stopped runtime source using SHA-256.
Fixture credentials/session data in these files belong only to the local lab.

Recompute cohort server timing:

```sh
python scripts/pos_bus/analyze_ramp_dispatch.py \
  EXTRACTED/results/cohorts-40k-02 --probe EXTRACTED/probe
```

Recompute resource attribution with `analyze_round.py` and each cohort directory,
using `--telemetry EXTRACTED/telemetry.jsonl --probe EXTRACTED/probe`. These tools
need no running Odoo server. The SQL audits require the retained fixture DBs.
The archived configs/launchers distinguish pool and memory settings and both
probe modes. Runtime records remain under
`/home/coder/playground/pos-bus-investigation/round2`.

The isolated server and observers were stopped after measurement. No upstream
Odoo code was patched, and no normal deployment or shared PostgreSQL settings
were changed. No 2,000-database or slow-reader result is claimed.
