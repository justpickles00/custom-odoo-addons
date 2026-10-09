# Browser-free POS/Bus laboratory

Results and limitations: [investigation](../../docs/pos-bus-investigation.md).
Pinned Odoo: `5bcf314eb0896bdf7e218341a134901004ba3d4d`.
Python used: `/home/coder/venvs/odoo/master/bin/python`, with aiohttp and psycopg2.

The retained fixture can be reused only after checking that no other task owns
its databases, ports or process. Do not run another Odoo server on the same
databases. The recorded launcher/config are in `docs/pos-bus-results/` and use
localhost ports 8061/8062. Their absolute paths are specific to this VM.
Start the isolated server explicitly when needed; it is not a system service.

For a new installation: initialize a fresh `posbus_template` with demo data and
`pos_restaurant,pos_bus_probe`, then execute `fixture.py` inside its Odoo shell.
The fixture refuses other database names and refuses a duplicate fixture.
Inspect the fixture output path/environment before running it. Stop that
template server before using `provision.py --count 20` (or 50/100); provisioning
clones the offline template and filestore. These are database writes.
Do not reuse unrelated databases or remove databases to make room.

`run.py --help` exposes workload parameters. The measured fleet cases are
defined in `matrix.py`; it runs them sequentially and preserves failed exits.
Each device has its own cookies and a native WebSocket. Originators use native
POS RPCs; followers issue native open-order/related-record reads. No browser is
launched. `orders.py` constructs the limited real order lifecycle; `client.py`
implements the inspected protocol/cache subset. See report fidelity boundaries.

Use a new output directory per run. For example, against the isolated server:

```sh
/home/coder/venvs/odoo/master/bin/python scripts/pos_bus/run.py \
  --fixture /home/coder/playground/pos-bus-investigation/fixture.json \
  --tenants 20 --duration 60 --rate .0125 --step-delay 1.2 \
  --startup-concurrency 2 --mode follower --output /tmp/posbus-new-run
```

`resources.py` samples process/host metrics; `guard.py --pid PID` bounds memory
risk by stopping only the explicitly identified experiment. The 10 GB lab's
registry and memory settings are constraints, not production recommendations.

After recorder flush, `analyze.py OUTPUT --probe PROBE_DIRECTORY` joins native
commits to socket receipts, including exact recipient sets and wholly missing
rows. `verify_persisted.py OUTPUT` independently checks real order lines, tax,
preparation and payments with read-only SQL. Preserve failed refreshes even when
all notifications arrive. `--verify-followers` enables selected intermediate
cache assertions; it was used in correctness smokes, not the fleet load runs.

`--coalesce-equal-sync` is a diagnostic client counterfactual, not native frontend
behavior. `--mode passive` suppresses follower reads. A stages-disabled probe
control is supported but was not run; no instrumentation-overhead claim is made.

Probe checks:

```sh
PYTHONPATH=/home/coder/worktrees/justpickles00/odoo/master \
  /home/coder/venvs/odoo/master/bin/python pos_bus_probe/tests/check_hooks.py
```
