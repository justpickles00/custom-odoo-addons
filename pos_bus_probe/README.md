# Isolated POS Bus probe

This experiment addon targets Odoo master commit
`5bcf314eb0896bdf7e218341a134901004ba3d4d`. It depends on `web` and `bus`.
Install it in the experiment databases and load it server-wide with
`--load=base,web,bus,pos_bus_probe`. Set `POS_BUS_PROBE_DIR` to an absolute output
directory. Hooks are absent when that variable is unset, and only databases
whose names start with `posbus_` are measured. Restart processes after changing
probe settings. `POS_BUS_PROBE_STAGES=0` retains commit/RPC recording and
disables all dispatch-stage wrappers for an overhead control.
Socket-close observations remain enabled in that control.

The authenticated JSON-RPC endpoint `/pos_bus_probe/publish` accepts
`channel`, `notification_type`, `payload`, and `operation_id`. All three string
arguments must be nonempty. It invokes native `bus.bus._sendone` with the exact
supplied payload and returns `{operation_id, queued: true}`. This endpoint is
test-only and returns 404 outside the enabled experiment databases. Native POS
RPCs use their ordinary routes; supply `X-Pos-Bus-Probe-Op` to correlate their
cursor commits. `/pos_bus_probe/flush` flushes the worker handling that request.

Records are buffered in memory and written to `probe-PID.jsonl` by a background
thread every 0.5 seconds. There are no per-event synchronous disk writes and no
fsync. The bounded queue holds 100,000 records; `recorder_stats.dropped_total`
must remain zero. Hard process termination can lose the unflushed tail.
In the gevent process the patched thread runs cooperatively: JSON encoding and
file writes still occupy the shared hub. Buffered recording is not zero-cost
instrumentation. High-rate conclusions require a stages-disabled comparison.
`recorder_stats` reports foreground recording time and asynchronous JSON/write
time. Logging errors are caught so they cannot turn a successful native commit
into a failed application operation. Records contain no business payloads.

All times are monotonic nanoseconds on the same VM. These are elapsed wall
times, not CPU time. Join commit and dispatch records by `(db, bus_id)`, since
row IDs overlap across tenants. The low-level connection commit wrapper samples
`commit_return_ns` immediately after PostgreSQL commit returns, before constructing
the record or running native postcommit NOTIFY callbacks. The timestamp is a
client-side observation of commit completion, not a PostgreSQL server timestamp.
Native precommit bus creation supplies the row IDs; full rollback and savepoint
rollback discard corresponding pending IDs.

Record kinds:

- `websocket_close`: database, process-local socket identity, native close code,
  reason and pending command count. Records the call before invoking native
  close unchanged; a saturated socket may shut down without transmitting its
  close reason to the client.

- `commit_return`: `db`, `operation_id`, `bus_ids`, `bus_rows`, `commit_start_ns`,
  `commit_return_ns`, `commit_duration_ns`.
  Each `bus_rows` item contains `id`, decoded native `channel`, `type`, and
  `payload_bytes` (native serialized payload bytes excluding the message envelope).
  This metadata permits detection of notifications missing from every recipient.
- `cursor_commit`: `db`, `operation_id`, `start_ns`, `end_ns`, `duration_ns`,
  `success`. Includes flush/precommit and native postcommit NOTIFY work.
- `rpc_dispatch`: `db`, `operation_id`, `path`, `start_ns`, `end_ns`,
  `duration_ns`. Excludes the final request transaction commit.
- `dispatch_batch`: `db`, `batch_id`, `start_ns`, `end_ns`, `duration_ns`,
  `claim_ns`, `topics`, `queue_wait_min_ns`, `queue_wait_max_ns`,
  `cursor_acquire_ns`, `session_validation_ns`, `fetch_ns`, `bus_ids`,
  `channels_with_rows`, `dispatch_ns`, `serialize_ns`, `serialized_bytes`,
  `forward_ns`, `forward_calls`, `enqueue_ns`, `websocket_send_calls`, `rollback`.
  Missing stage counters mean zero/unreached. Queue wait is from each topic's
  native enqueue to completion of the batch claim, including lock acquisition.
  Cursor acquisition includes native retries/yields. Serialization and enqueue
  are subsets of forwarding, which is a subset of dispatch; do not sum these
  overlapping durations. `bus_ids` are fetched IDs, so history/catchup filtering
  can remove some before send. `websocket_send_calls` counts native send attempts,
  including a socket that closes concurrently. Enqueue ends at the native send
  method; actual socket write and recipient receipt occur later.
- `recorder_stats`: `dropped_total`, `emit_count_total`, `emit_ns_total`,
  `encode_ns`, `write_ns`, `record_count`, `stages_enabled`. Total counters are
  cumulative per process; encode/write counters apply to that flush batch.

Every record also has `kind`, `pid`, and `monotonic_ns`. Dispatch instrumentation
adds clock reads and bookkeeping; use the stages-disabled control and client
latency measurements to quantify perturbation. Do not describe elapsed spans
as CPU consumption.

Validation: `PYTHONPATH=/path/to/odoo /path/to/venv/bin/python
pos_bus_probe/tests/check_hooks.py` runs database-free semantic checks. Real
isolated template installation also verified commit ID correlation and savepoint
rollback. The experiment template has all 24 initially active `ir.cron` jobs
disabled; clones inherit this idle-cron control. Configure the server's explicit
`db_name` list to the isolated fixture databases so its cron process cannot
enumerate unrelated databases. A dbfilter alone does not constrain cron scanning.
