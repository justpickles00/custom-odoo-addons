# Evidence index

Read the [investigation](../pos-bus-investigation.md) first. These are isolated,
browser-free experiments against pinned Odoo master, not production capacity results.

Each completed fleet directory contains `summary.json` (client counters and
timings), `analysis.json` (commit-to-receipt joins and exact recipient checks),
and `persisted-verification.json` (independent read-only PostgreSQL order audit).

| Directory | Purpose |
| --- | --- |
| `fleet-20-paced` | Initial 120-second native baseline, larger HTTP memory limit |
| `fleet-50-bounded` | 50 databases with bounded process memory |
| `fleet-100-bounded` | Native 100-database run: all notifications delivered, 13 follower HTTP failures |
| `fleet-100-coalesced` | Diagnostic duplicate-callback suppression; server messages unchanged |
| `fleet-20-passive` | Record notifications without follower reads |
| `fleet-20-native-repeat` | Native follower control under bounded settings |

`raw-evidence.tar.gz` contains cumulative per-process probe JSONL, all client
streams (including smokes and unsuccessful startup runs), resource samples,
guard/matrix logs and configuration. Its SHA-256 is in `manifest.json`.
All JSONL entries were parsed after packaging. Failed/interrupted runs remain
in the archive for diagnosis and are not counted as successful capacity tests.

`odoo.conf` and `run-server.sh` preserve the final bounded configuration;
the main report records the earlier settings. `fixture.json` and
`installed-modules.txt` identify the synthetic fixture. Credentials/tokens in
the fixture belong only to this isolated laboratory. Do not deploy its probe
endpoint to a normal Odoo environment.

To reproduce the analysis, extract the archive in a new scratch directory and
run `scripts/pos_bus/analyze.py EXTRACTED/results/CASE --probe EXTRACTED/probe`.
This needs no server or database. Re-running the persisted-order audit requires
the retained `posbus_` databases; it opens read-only PostgreSQL transactions.

The fixture databases are retained locally: `posbus_template` and
`posbus_0000` through `posbus_0099`, with their filestores under
`/home/coder/playground/pos-bus-investigation/data`. The experiment's server
was stopped after the measurements. Upstream Odoo was not patched.
