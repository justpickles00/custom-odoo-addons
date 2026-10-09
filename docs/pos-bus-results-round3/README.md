# Real POS workload evidence

Read the [report](../pos-bus-realistic-load.md) for conclusions, source references,
and fidelity/configuration limits. All workloads used task-owned fixture databases
and no browsers. The original SSE worktree and upstream Odoo source were unchanged.

| Case | Classification |
| --- | --- |
| `smoke` | Excluded setup: new order merged with retained draft; no measurement window |
| `smoke-corrected` | Passed four-order fidelity smoke |
| `restaurants-4-paced` | Passed native baseline: 288 actions, 32 verified orders |
| `restaurants-20-paced` | Passed native baseline: 1,440 actions, 160 verified orders |
| `restaurants-20-busy-native` | Failed 40-actions/s workload; HTTP errors, follower cap and unsafe continuation after errors |
| `restaurants-20-busy-coalesced` | Failed diagnostic at same offered rate; no portable capacity/speedup conclusion |
| `restaurants-20-medium-native` | Failed 16-actions/s workload; corrected per-till fail-stop |
| `restaurants-20-medium-coalesced` | Passed diagnostic: all 1,440 actions including one boundary drain completion, 160 verified orders |
| `restaurants-50-paced` | Host-memory guard stopped startup after 116 devices; no orders or measured window |
| `cleanup-*` | Separate native cancellation/reconciliation of interrupted owned orders; not workload acceptance |

`analysis.json` checks exact committed-row recipients and delivery latency.
`restaurant-report.json` separates offered actions, queue admissions, errors,
abandonment, callback suppression, verification and sampled resource limits.
`host-analysis.json` exists only for the medium coalesced control: the additional
counters were not recorded during earlier experiments.

The readable checked-in summaries omit large repeated order graphs; the raw
archives preserve complete owned/expected-order files and cleanup summaries.

Four archives reconstruct the original runtime layout when extracted into the
same scratch directory:

- `workloads-raw.tar.gz`: complete per-case client streams and outputs.
- `cleanup-raw.tar.gz`: authoritative before snapshots, exact ownership and native RPC responses.
- `probe-raw.tar.gz`: server commit, delivery-stage and recorder evidence.
- `runtime-raw.tar.gz`: telemetry, host counters, logs, configurations and guard records.

`manifest.json` records archive sizes and SHA-256 values. `raw-file-hashes.json`
records the hash of every source file. Every archived member was decompressed
and checked against its stopped runtime source. Some raw traces contain local
fixture credentials/session data; they concern only this isolated laboratory.

Recompute a case, without starting Odoo or opening a browser:

```sh
python scripts/pos_bus/analyze.py EXTRACTED/results/CASE --probe EXTRACTED/probe
python scripts/pos_bus/analyze_round.py EXTRACTED/results/CASE \
  --probe EXTRACTED/probe --telemetry EXTRACTED/telemetry.jsonl
python scripts/pos_bus/restaurant_report.py EXTRACTED/results/CASE \
  --probe EXTRACTED/probe --telemetry EXTRACTED/telemetry.jsonl
```

Use the master-series virtual environment noted in the script README. Startup
failures have no measurement window and must not be processed as measured cases.
The live originals remain under `/home/coder/playground/pos-bus-investigation/round3`.
All experiment servers and observers are stopped. PostgreSQL settings were unchanged.
