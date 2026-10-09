#!/usr/bin/env python3
"""Deterministic, bounded asyncio load generator for isolated POS bus fixtures.

Example: python scripts/pos_bus/run.py --fixture fixture.json --tenants 1
    --tills 2 --background 0 --duration 15 --rate .05 --mode follower
Rate is complete order lifecycles per second per till, not individual RPCs.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
import json
import math
from pathlib import Path
import time
import uuid

from client import Device, Metrics
from orders import Order


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--http-url", default="http://127.0.0.1:8061")
    parser.add_argument("--ws-url", default="ws://127.0.0.1:8062")
    parser.add_argument("--db-prefix", default="posbus_")
    parser.add_argument("--db-start", type=int, default=0)
    parser.add_argument("--tenants", type=int, default=1)
    parser.add_argument("--tills", type=int, default=4)
    parser.add_argument("--background", type=int, default=2)
    parser.add_argument("--restaurant-fraction", type=float, default=.2)
    parser.add_argument("--mode", choices=("passive", "follower", "synthetic"), default="follower")
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--rate", type=float, default=.05)
    parser.add_argument("--step-delay", type=float, default=.25)
    parser.add_argument("--all-active", action="store_true", help="Control experiment: activate tills in every tenant")
    parser.add_argument("--verify-followers", action="store_true", help="Correctness smoke: await and assert other tills' intermediate caches after each native step")
    parser.add_argument("--verify-timeout", type=float, default=3)
    parser.add_argument("--canary-interval", type=float, default=30, help="Per-tenant monitoring canary period; zero disables during-load canaries")
    parser.add_argument("--coalesce-equal-sync", action="store_true", help="COUNTERFACTUAL: coalesce equal native follower callbacks only within one websocket frame; preserve every raw receipt")
    parser.add_argument("--cancel-fraction", type=float, default=.2)
    parser.add_argument("--synthetic-bytes", type=int, default=256)
    parser.add_argument("--queue-size", type=int, default=32)
    parser.add_argument("--cache-limit", type=int, default=10000)
    parser.add_argument("--closed-cache", type=int, default=32)
    parser.add_argument("--startup-concurrency", type=int, default=12)
    parser.add_argument("--follower-concurrency", type=int, default=8, help="Bound concurrent native async notification callbacks per device")
    parser.add_argument("--rpc-timeout", type=float, default=60)
    parser.add_argument("--barrier-timeout", type=float, default=30)
    parser.add_argument("--drain-timeout", type=float, default=60)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.tenants < 1 or args.tills < 1 or args.background < 0 or args.rate <= 0 or args.duration <= 0 or args.queue_size < 1 or args.startup_concurrency < 1 or args.follower_concurrency < 1:
        parser.error("counts, duration, rate and queue size must be positive (background may be zero)")
    if not 0 <= args.restaurant_fraction <= 1 or not 0 <= args.cancel_fraction <= 1:
        parser.error("fractions must be between zero and one")
    if min(args.step_delay, args.closed_cache, args.canary_interval) < 0 or args.cache_limit < 1:
        parser.error("delays and closed cache must be nonnegative; cache limit must be positive")
    args.run_id = uuid.uuid4().hex[:12]
    args.output = args.output or Path("/home/coder/playground/pos-bus-results") / args.run_id
    return args


def fixtures_from_manifest(manifest, args):
    if "tenants" in manifest:
        return manifest["tenants"][:args.tenants]
    # Support one fixture or role-specific templates with stable IDs in clones.
    restaurant_count = max(1, round(args.tenants * args.restaurant_fraction)) if args.restaurant_fraction else 0
    fixtures = []
    for index in range(args.tenants):
        restaurant = index < restaurant_count
        fixture = dict(manifest.get("restaurant" if restaurant else "retail", manifest))
        fixture["db"] = f"{args.db_prefix}{args.db_start + index:04d}"
        fixture["restaurant"] = restaurant
        fixtures.append(fixture)
    return fixtures


async def barrier(devices, phase, args, metrics):
    """Positive per-socket proof of subscription; startup timing is no proxy."""
    by_db = defaultdict(list)
    for device in devices:
        by_db[device.db].append(device)
    async def tenant_barrier(group):
        token = f"{args.run_id}:{phase}:{group[0].db}"
        for device in group:
            device.canaries[token] = asyncio.Event()
        try:
            await group[0].publish("posbus.canary", {"token": token}, token, channel=group[0].canary_channel)
            await asyncio.wait_for(asyncio.gather(*(device.canaries[token].wait() for device in group)), timeout=args.barrier_timeout)
            metrics.event("barrier_passed", db=group[0].db, phase=phase, clients=len(group))
        except Exception:
            for device in group:
                if not device.canaries[token].is_set():
                    metrics.event("missing_canary", device=device.name, db=device.db, phase=phase, token=token)
            raise
        finally:
            for device in group:
                device.canaries.pop(token, None)
    await asyncio.gather(*(tenant_barrier(group) for group in by_db.values()))


async def lifecycle(device, sequence, scheduled_ns, args, metrics):
    operation = f"{args.run_id}:{device.name}:order:{sequence}"
    started = time.monotonic_ns()
    metrics.event("lifecycle_start", device=device.name, db=device.db, operation_id=operation, sequence=sequence, scheduled_ns=scheduled_ns, started_ns=started, scheduling_lag_ns=started - scheduled_ns)
    metrics.observe("scheduling_lag", (started - scheduled_ns) / 1e6)
    if args.mode == "synthetic":
        await device.publish("posbus.synthetic", {"operation_id": operation, "padding": "x" * args.synthetic_bytes}, operation)
        metrics.event("lifecycle_complete", device=device.name, db=device.db, operation_id=operation, synthetic=True)
        return
    order = Order(device.fixture, worker=f"{args.run_id}-{device.name}", sequence=sequence, table_index=device.till_index, user_id=device.fixture["users"][device.till_index % len(device.fixture["users"])]["id"])
    steps = ["create", "edit"]
    if device.fixture.get("restaurant", True):
        steps.append("prepare")
    # Deterministic cancellation fraction, spread over sequence and device.
    cancel = ((sequence * 37 + device.till_index * 19) % 100) < args.cancel_fraction * 100
    steps.append("cancel" if cancel else "pay")
    for index, step in enumerate(steps):
        if index and args.step_delay:
            await asyncio.sleep(args.step_delay)
        op = operation + ":" + step
        if step == "cancel":
            response = await device.call("pos.order", "cancel_order_from_pos", [[order.id]], {"context": order.context(device.identifier)}, op)
        else:
            payload = getattr(order, step)()
            response = await device.call("pos.order", "sync_from_ui", [[payload]], {"context": order.context(device.identifier)}, op)
        order.apply_response(response)
        order.assert_response(response, "cancel" if step == "cancel" else "paid" if step == "pay" else "draft")
        if args.mode == "follower":
            await device.missing_recursive(response)
            device.prune_closed()
        metrics.event("order_step", device=device.name, db=device.db, operation_id=op, order_id=order.id, step=step)
        if args.verify_followers and args.mode == "follower":
            await verify_followers(device, order, step, op, args, metrics)
    metrics.event("lifecycle_complete", device=device.name, db=device.db, operation_id=operation, order_id=order.id, final_state="cancel" if cancel else "paid", elapsed_ns=time.monotonic_ns() - started)


async def verify_followers(origin, order, step, operation_id, args, metrics):
    expected_state = "cancel" if step == "cancel" else "paid" if step == "pay" else "draft"
    async def verify(peer):
        started = time.monotonic()
        actual = {}
        while True:
            row = peer.cache["pos.order"].get(order.id)
            line_rows = [peer.cache["pos.order.line"].get(line["id"]) for line in order.lines]
            prep = [row for row in peer.cache["pos.prep.order"].values() if row.get("uuid") == order._uuid("prep-order")]
            prep_ids = {row["id"] for row in prep}
            prep_lines = [row for row in peer.cache["pos.prep.line"].values() if row.get("prep_order_id") in prep_ids]
            payments = [row for row in peer.cache["pos.payment"].values() if row.get("pos_order_id") == order.id]
            quantities = [row.get("qty") if row else None for row in line_rows]
            graph = {line["pos_order_line_id"]: line["quantity"] for line in prep_lines}
            preparation_correct = not order.prepared or bool(prep and all(graph.get(line["id"]) == line["qty"] for line in order.lines))
            payment_correct = expected_state != "paid" or abs(sum(payment["amount"] for payment in payments) - order.totals["total"]) < .005
            actual = {"state": row.get("state") if row else None, "quantities": quantities, "preparation_ids": list(prep_ids), "preparation_graph": graph, "payment_ids": [payment["id"] for payment in payments], "payment_total": sum(payment["amount"] for payment in payments)}
            correct = row is None if expected_state == "cancel" else bool(row and row.get("state") == expected_state and quantities == [line["qty"] for line in order.lines] and preparation_correct and payment_correct)
            if correct or time.monotonic() - started >= args.verify_timeout:
                metrics.event("follower_assertion" if correct else "follower_assertion_error", device=peer.name, origin=origin.name, db=peer.db, operation_id=operation_id, order_id=order.id, step=step, expected_state=expected_state, expected_quantities=[line["qty"] for line in order.lines], actual=actual, elapsed_s=time.monotonic() - started)
                return
            await asyncio.sleep(.02)
    await asyncio.gather(*(verify(peer) for peer in origin.peers if peer is not origin and peer.role == "till"))


async def worker(device, queue, args, metrics):
    while True:
        sequence, scheduled_ns = await queue.get()
        try:
            await lifecycle(device, sequence, scheduled_ns, args, metrics)
        except asyncio.CancelledError:
            metrics.event("lifecycle_cancelled", device=device.name, sequence=sequence)
            raise
        except Exception as error:
            metrics.event("lifecycle_error", device=device.name, db=device.db, sequence=sequence, error=str(error))
        finally:
            queue.task_done()


async def scheduler(device, queue, start, args, metrics):
    period = 1 / args.rate
    # Till phases cover the whole order period; tenant offsets interleave
    # restaurants inside each till phase, preserving steady low offered rates.
    offset = device.schedule_phase * period
    sequence = 0
    while True:
        deadline = start + offset + sequence * period
        if deadline >= start + args.duration:
            break
        await asyncio.sleep(max(0, deadline - time.monotonic()))
        scheduled_ns = int(deadline * 1e9)
        metrics.event("lifecycle_scheduled", device=device.name, db=device.db, sequence=sequence, scheduled_ns=scheduled_ns, scheduler_ns=time.monotonic_ns())
        try:
            queue.put_nowait((sequence, scheduled_ns))
        except asyncio.QueueFull:
            metrics.event("schedule_queue_full", device=device.name, db=device.db, sequence=sequence, scheduled_ns=scheduled_ns)
        sequence += 1


async def periodic_canary(group, tenant_index, tenant_count, start, args, metrics):
    sequence = 0
    # Each tenant publishes once per interval, with the first publication
    # evenly distributed across that interval. No synchronized fleet burst.
    offset = (tenant_index + 1) / tenant_count * args.canary_interval
    while args.canary_interval:
        deadline = start + offset + sequence * args.canary_interval
        if deadline >= start + args.duration:
            return
        await asyncio.sleep(max(0, deadline - time.monotonic()))
        token = f"{args.run_id}:monitor:{group[0].db}:{sequence}"
        metrics.event("monitor_canary_publish", db=group[0].db, operation_id=token, token=token, sequence=sequence, scheduled_ns=int(deadline * 1e9), expected_devices=[device.name for device in group])
        try:
            await group[0].publish("posbus.canary", {"token": token, "phase": "monitor", "sequence": sequence}, token, channel=group[0].canary_channel)
        except Exception as error:
            metrics.event("monitor_canary_error", db=group[0].db, operation_id=token, error=str(error))
        sequence += 1


async def progress(metrics, state):
    while True:
        await asyncio.sleep(5)
        metrics.flush()
        print(json.dumps({"progress": state["phase"], "elapsed_s": round(time.monotonic() - state["start"], 1), "counts": dict(metrics.counts)}), flush=True)


async def monitor_event_loop(metrics, state, interval=.05):
    deadline = time.monotonic() + interval
    while True:
        await asyncio.sleep(max(0, deadline - time.monotonic()))
        actual = time.monotonic()
        lag_ms = max(0, actual - deadline) * 1000
        metrics.observe("event_loop_lag." + state["phase"], lag_ms)
        metrics.counts["event_loop_samples"] += 1
        if lag_ms > 50:
            metrics.event("event_loop_stall", phase=state["phase"], lag_ms=lag_ms, scheduled_ns=int(deadline * 1e9))
        # Avoid a burst of catch-up samples after a blocked event loop.
        deadline = actual + interval if actual - deadline >= interval else deadline + interval


async def run(args):
    manifest = json.loads(args.fixture.read_text())
    fixtures = fixtures_from_manifest(manifest, args)
    if len(fixtures) != args.tenants:
        raise ValueError("manifest has fewer tenants than requested")
    metrics = Metrics(args.output)
    state = {"phase": "startup", "start": time.monotonic()}
    status = asyncio.create_task(progress(metrics, state))
    loop_monitor = asyncio.create_task(monitor_event_loop(metrics, state))
    devices, work_tasks, startup_tasks = [], [], []
    summary = {}
    try:
        metrics.event("run_start", run_id=args.run_id, native=not args.coalesce_equal_sync, args=vars(args), tenants=[{"db": f["db"], "restaurant": f.get("restaurant", True)} for f in fixtures])
        semaphore = asyncio.Semaphore(args.startup_concurrency)
        async def start_device(device):
            async with semaphore:
                await device.start()
        active_fixtures = [fixture for fixture in fixtures if fixture.get("restaurant", True) or args.all_active]
        active_tenant_index = 0
        for fixture in fixtures:
            active_tills = args.tills if fixture.get("restaurant", True) or args.all_active else 0
            for index in range(active_tills + args.background):
                role = "till" if index < active_tills else "background"
                device = Device(fixture, f"{fixture['db']}:{role}:{index}", role, args, metrics)
                device.till_index = index
                device.schedule_index = len(devices)
                device.schedule_phase = (index / args.tills + active_tenant_index / max(1, len(active_fixtures)) / args.tills) % 1 if role == "till" else 0
                devices.append(device)
            if active_tills:
                active_tenant_index += 1
        for device in devices:
            device.peers = [peer for peer in devices if peer.db == device.db]
        startup_tasks = [asyncio.create_task(start_device(device), name=device.name + ":startup") for device in devices]
        await asyncio.gather(*startup_tasks)
        await barrier(devices, "ready", args, metrics)
        state["phase"] = "load"
        start = time.monotonic() + .1
        await asyncio.sleep(max(0, start - time.monotonic()))
        metrics.event("measurement_start", run_id=args.run_id, scheduled_start_ns=int(start * 1e9), offered_lifecycle_count=sum(max(0, math.ceil(args.duration * args.rate - device.schedule_phase)) for device in devices if device.role == "till"))
        queues, schedules = [], []
        for device in devices:
            if device.role != "till":
                continue
            queue = asyncio.Queue(args.queue_size)
            queues.append(queue)
            work_tasks.append(asyncio.create_task(worker(device, queue, args, metrics)))
            schedules.append(asyncio.create_task(scheduler(device, queue, start, args, metrics)))
        if args.canary_interval:
            by_db = defaultdict(list)
            for device in devices:
                by_db[device.db].append(device)
            schedules.extend(asyncio.create_task(periodic_canary(group, index, len(by_db), start, args, metrics)) for index, group in enumerate(by_db.values()))
        await asyncio.sleep(max(0, start + args.duration - time.monotonic()))
        metrics.event("measurement_end", run_id=args.run_id, scheduled_end_ns=int((start + args.duration) * 1e9))
        await asyncio.gather(*schedules)
        state["phase"] = "drain"
        try:
            await asyncio.wait_for(asyncio.gather(*(queue.join() for queue in queues)), args.drain_timeout)
        except asyncio.TimeoutError:
            metrics.event("work_drain_timeout", unfinished=sum(queue.qsize() for queue in queues))
        await barrier(devices, "final", args, metrics)
        try:
            await asyncio.wait_for(asyncio.gather(*(device.notifications.join() for device in devices)), args.drain_timeout)
        except asyncio.TimeoutError:
            metrics.event("follower_drain_timeout")
        for device in devices:
            metrics.event("cache_snapshot", device=device.name, db=device.db, records={model: len(rows) for model, rows in device.cache.items()}, orders=list(device.cache["pos.order"].values()))
        await devices[0].rpc("/pos_bus_probe/flush", {})
        metrics.event("run_complete", run_id=args.run_id)
    except Exception as error:
        metrics.event("run_error", error=str(error))
        raise
    finally:
        # gather propagates the first startup failure without cancelling its
        # siblings. Stop and await every startup task before closing devices,
        # so a pending start cannot open a session after cleanup has passed it.
        for task in startup_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*startup_tasks, return_exceptions=True)
        for task in work_tasks:
            task.cancel()
        await asyncio.gather(*work_tasks, return_exceptions=True)
        await asyncio.gather(*(device.close() for device in devices), return_exceptions=True)
        status.cancel()
        loop_monitor.cancel()
        await asyncio.gather(status, loop_monitor, return_exceptions=True)
        summary = {"run_id": args.run_id, "native": not args.coalesce_equal_sync, "elapsed_s": time.monotonic() - state["start"], **metrics.summary()}
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        metrics.close()
        print(json.dumps({"output": str(args.output), **summary}), flush=True)
    bad = ("lifecycle_error", "rpc_error", "ws_disconnect", "notification_queue_full", "schedule_queue_full", "follower_error", "missing_canary", "work_drain_timeout", "follower_drain_timeout", "follower_assertion_error", "monitor_canary_error")
    return 1 if any(summary["counts"].get(key, 0) for key in bad) else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(arguments())))
