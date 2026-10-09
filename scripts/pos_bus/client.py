"""Browser-free Odoo POS RPC and native bus websocket client.

Protocol and follower domains follow the pinned master frontend.  No browser,
asset execution, or shared cookie jar is involved.
"""
from __future__ import annotations

import asyncio
from collections import Counter, defaultdict, OrderedDict
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import time
from typing import Any

import aiohttp


class Metrics:
    """Fixed logarithmic histograms and streaming JSONL keep memory bounded."""

    BOUNDS = tuple(0.125 * 2**i for i in range(24))  # milliseconds

    def __init__(self, output: Path):
        output.mkdir(parents=True, exist_ok=True)
        self.output = output
        self.stream = (output / "clients.jsonl").open("w", buffering=1024 * 1024)
        self.counts = Counter()
        self.histograms = defaultdict(lambda: [0] * (len(self.BOUNDS) + 1))
        self.sums = Counter()
        self.maximum = Counter()

    def event(self, kind: str, **values):
        self.counts[kind] += 1
        self.stream.write(json.dumps({"event": kind, "monotonic_ns": time.monotonic_ns(), **values}, separators=(",", ":"), default=str) + "\n")

    def observe(self, name: str, ms: float):
        index = min(len(self.BOUNDS), max(0, math.ceil(math.log2(max(ms, 0.125) / 0.125))))
        self.histograms[name][index] += 1
        self.sums[name] += ms
        self.maximum[name] = max(self.maximum[name], ms)

    def summary(self):
        hist = {}
        for name, buckets in self.histograms.items():
            count = sum(buckets)
            values = {"count": count, "mean_ms": self.sums[name] / count if count else None, "max_ms": self.maximum[name]}
            for quantile in (0.5, 0.95, 0.99):
                threshold, cumulative = math.ceil(count * quantile), 0
                for index, bucket in enumerate(buckets):
                    cumulative += bucket
                    if cumulative >= threshold:
                        values[f"p{int(quantile * 100)}_upper_ms"] = self.BOUNDS[index] if index < len(self.BOUNDS) else None
                        break
            hist[name] = values
        return {"counts": dict(self.counts), "histograms": hist}

    def flush(self):
        self.stream.flush()

    def close(self):
        self.stream.close()


class RPCError(RuntimeError):
    pass


class Device:
    DYNAMIC = ("pos.order", "pos.order.line", "pos.payment", "product.attribute.custom.value", "pos.prep.order", "pos.prep.line", "restaurant.order.course")
    PROHIBITED = {"pos.order", "pos.order.line", "pos.session", "pos.config", "res.users", "account.tax"}

    def __init__(self, fixture: dict, name: str, role: str, args, metrics: Metrics):
        self.fixture, self.name, self.role, self.args, self.metrics = fixture, name, role, args, metrics
        self.db = fixture["db"]
        self.identifier = None
        self.last_id = 0
        self.http = None
        self.ws = None
        self.reader_task = None
        self.follow_tasks = []
        self.heartbeat_task = None
        self.follow_active = 0
        self.seen_ids = OrderedDict()
        self.canary_channel = "posbus.readiness"
        self.notifications = asyncio.Queue(args.queue_size)
        self.cache = defaultdict(OrderedDict)
        self.relations = {}
        self.canaries = {}
        self.closed = False
        self.rpc_sequence = 0
        self.follow_pending_ns = None

    async def rpc(self, route: str, params: dict, operation_id: str | None = None):
        self.rpc_sequence += 1
        operation_id = operation_id or f"{self.args.run_id}:{self.name}:rpc:{self.rpc_sequence}"
        start = time.monotonic_ns()
        method = params.get("method", route)
        self.metrics.event("rpc_start", device=self.name, db=self.db, operation_id=operation_id, method=method, model=params.get("model"))
        try:
            async with self.http.post(self.args.http_url + route, json={"jsonrpc": "2.0", "method": "call", "id": self.rpc_sequence, "params": params}, headers={"X-Pos-Bus-Probe-Op": operation_id}) as response:
                response.raise_for_status()
                data = await response.json()
            if "error" in data:
                raise RPCError(json.dumps(data["error"], default=str))
            result = data.get("result")
            self.metrics.event("rpc_end", device=self.name, db=self.db, operation_id=operation_id, method=method, elapsed_ns=time.monotonic_ns() - start)
            return result
        except Exception as error:
            self.metrics.event("rpc_error", device=self.name, db=self.db, operation_id=operation_id, method=method, error=str(error))
            raise
        finally:
            self.metrics.observe("rpc." + method, (time.monotonic_ns() - start) / 1e6)

    async def call(self, model, method, args, kwargs=None, operation_id=None):
        return await self.rpc(f"/web/dataset/call_kw/{model}/{method}", {"model": model, "method": method, "args": args, "kwargs": kwargs or {}}, operation_id)

    async def start(self):
        # IP hosts require unsafe=True; each device owns an independent jar.
        self.http = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True), timeout=aiohttp.ClientTimeout(total=self.args.rpc_timeout), connector=aiohttp.TCPConnector(limit=self.args.follower_concurrency + 4))
        users = self.fixture.get("users", [{}])
        user = users[self.till_index % len(users)]
        info = await self.rpc("/web/session/authenticate", {"db": self.db, "login": user.get("login", "admin"), "password": user.get("password", "admin")})
        if not info or not info.get("uid"):
            raise RPCError(f"Authentication failed: {self.db}")
        info = await self.rpc("/web/session/get_session_info", {})
        self.user_context = info.get("user_context", {})
        if self.role == "till":
            result = await self.call("pos.config", "register_new_device_identifier", [[self.fixture["config_id"]]])
            self.identifier = result["device_identifier"]
        if self.args.mode == "follower" and self.role == "till":
            bootstrap = await self.call("pos.session", "load_data", [[self.fixture["session_id"]], {}])
            for model, metadata in bootstrap.items():
                self.relations[model] = metadata.get("relations", {})
                self.merge({model: metadata.get("records", [])})
        bus_info = info["bus_info"]
        self.last_id = bus_info.get("last_id") or 0
        ws_url = self.args.ws_url + "/websocket?version=" + str(bus_info["worker_version"])
        self.ws = await self.http.ws_connect(ws_url, autoping=True, origin=self.args.http_url)
        channels = [self.canary_channel]
        if self.role == "till":
            channels.append(self.fixture["access_token"])
        await self.ws.send_json({"event_name": "subscribe", "data": {"channels": channels, "last": self.last_id, "check_outdated": True}})
        self.reader_task = asyncio.create_task(self.read_notifications(), name=self.name + ":ws")
        self.follow_tasks = [asyncio.create_task(self.follow_notifications(), name=self.name + f":follow:{index}") for index in range(self.args.follower_concurrency)] if self.args.mode == "follower" and self.role == "till" else []
        self.heartbeat_task = asyncio.create_task(self.heartbeat(), name=self.name + ":heartbeat")
        self.metrics.event("device_started", device=self.name, db=self.db, role=self.role, device_identifier=self.identifier, last_id=self.last_id)

    async def publish(self, notification_type, payload, operation_id, channel=None):
        return await self.rpc("/pos_bus_probe/publish", {"channel": channel or self.fixture["access_token"], "notification_type": notification_type, "payload": payload, "operation_id": operation_id}, operation_id)

    async def heartbeat(self):
        while True:
            await asyncio.sleep(45)
            if self.ws.closed:
                return
            await self.ws.send_bytes(b"\x00")
            self.metrics.event("ws_heartbeat", device=self.name)

    async def read_notifications(self):
        try:
            async for frame in self.ws:
                received = time.monotonic_ns()
                if frame.type == aiohttp.WSMsgType.TEXT:
                    self.metrics.event("ws_raw", device=self.name, db=self.db, received_ns=received, raw=frame.data)
                    try:
                        notifications = json.loads(frame.data)
                    except (ValueError, TypeError) as error:
                        self.metrics.event("decode_error", device=self.name, error=str(error))
                        continue
                    if not isinstance(notifications, list):
                        self.metrics.event("control_frame", device=self.name, value=notifications)
                        continue
                    frame_callbacks = set()
                    for notification in notifications:
                        if notification.get("internal"):
                            control_type = notification.get("type")
                            self.metrics.event("bus_control", device=self.name, type=control_type, payload=notification.get("payload"))
                            if control_type == "bus/last_id_reset":
                                self.last_id = notification["payload"]
                                self.seen_ids = OrderedDict((key, None) for key in self.seen_ids if key <= self.last_id)
                            elif control_type == "bus/subscription_outdated":
                                self.metrics.event("subscription_outdated", device=self.name)
                            continue
                        message = notification.get("message", {})
                        bus_id = notification.get("id", 0)
                        if bus_id in self.seen_ids:
                            self.metrics.event("duplicate_notification", device=self.name, db=self.db, bus_id=bus_id)
                            continue
                        self.seen_ids[bus_id] = None
                        if len(self.seen_ids) > 10000:
                            self.seen_ids.popitem(last=False)
                        self.last_id = max(self.last_id, bus_id)
                        self.metrics.event("notification", device=self.name, db=self.db, id=bus_id, bus_id=bus_id, mono_ns=received, received_ns=received, type=message.get("type"), notification_type=message.get("type"), payload_bytes=len(json.dumps(message.get("payload"), separators=(",", ":")).encode()), payload=message.get("payload"))
                        payload = message.get("payload") or {}
                        if message.get("type") == "posbus.canary" and isinstance(payload, dict):
                            token = payload.get("token")
                            if token in self.canaries:
                                self.canaries[token].set()
                        if self.args.mode == "follower" and self.role == "till" and message.get("type") in (self.fixture["access_token"] + "-SYNCHRONISATION", self.fixture["access_token"] + "-FLOOR_PLAN_UPD"):
                            callback_key = json.dumps(message, sort_keys=True, separators=(",", ":"))
                            if self.args.coalesce_equal_sync and callback_key in frame_callbacks:
                                self.metrics.event("coalesced_follower_callback", device=self.name, db=self.db, bus_id=bus_id, notification_type=message.get("type"))
                                continue
                            frame_callbacks.add(callback_key)
                            try:
                                if self.follow_active >= self.args.follower_concurrency:
                                    self.metrics.event("follower_saturation", device=self.name, queue_size=self.notifications.qsize())
                                self.notifications.put_nowait((message, received))
                            except asyncio.QueueFull:
                                self.metrics.event("notification_queue_full", device=self.name, bus_id=bus_id)
                elif frame.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError(str(self.ws.exception()))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.metrics.event("ws_error", device=self.name, db=self.db, error=str(error))
        finally:
            if not self.closed:
                self.metrics.event("ws_disconnect", device=self.name, db=self.db, close_code=self.ws.close_code)

    def merge(self, records):
        for model, values in records.items():
            cache = self.cache[model]
            for value in values:
                if not isinstance(value, dict) or not value.get("id"):
                    continue
                cache[value["id"]] = {**cache.get(value["id"], {}), **value}
                cache.move_to_end(value["id"])
            while len(cache) > self.args.cache_limit:
                cache.popitem(last=False)
                self.metrics.counts["cache_eviction"] += 1

    def remove(self, deleted):
        for model, ids in (deleted or {}).items():
            for record_id in ids:
                self.cache[model].pop(record_id, None)
                if model == "pos.order":
                    for child, relation in (("pos.order.line", "order_id"), ("pos.payment", "pos_order_id"), ("product.attribute.custom.value", "pos_order_id")):
                        for key, row in list(self.cache[child].items()):
                            if row.get(relation) == record_id:
                                del self.cache[child][key]

    async def missing_recursive(self, records):
        self.merge(records)
        seen = defaultdict(set)
        for depth in range(12):
            missing = defaultdict(set)
            for model, rows in records.items():
                for field, relation in self.relations.get(model, {}).items():
                    target = relation.get("relation")
                    if not target or target in self.PROHIBITED or target not in self.relations or (model == "res.partner" and field == "property_product_pricelist"):
                        continue
                    for row in rows:
                        value = row.get(field)
                        values = value if isinstance(value, list) else [value]
                        for record_id in values:
                            if isinstance(record_id, int) and not isinstance(record_id, bool) and record_id and record_id not in self.cache[target] and record_id not in seen[target]:
                                missing[target].add(record_id)
            if not missing:
                return
            params = {"models": list(missing), "records": {}, "only_records": True, "search_params": {model: {"domain": [["id", "in", sorted(ids)]]} for model, ids in missing.items()}}
            # Product template / variant coupling matches getRelatedModels.
            for model in ("product.product", "product.template"):
                if model in missing:
                    other = "product.template" if model == "product.product" else "product.product"
                    params["models"].append(other)
                    params["search_params"][model]["context"] = {"active_test": False}
                    params["search_params"][other] = {"domain": [["product_variant_ids" if model == "product.product" else "product_tmpl_id", "in", sorted(missing[model])]], "context": {"active_test": False}}
            for model, ids in missing.items():
                seen[model].update(ids)
            records = await self.call("pos.session", "load_data", [[self.fixture["session_id"]], params])
            self.merge(records)
        self.metrics.event("relation_depth_limit", device=self.name)

    def open_orders_domain(self):
        # Literal native databaseTable condition uses pos_session_id (not the
        # server field session_id); finalized records therefore do not enter
        # constructOrdersDomain. Retained closed cache is inspection history.
        rows = [row for row in self.cache["pos.order"].values() if row.get("state", "draft") == "draft"]
        ids = [row["id"] for row in rows]
        domains = []
        for row in rows:
            if not row.get("write_date"):
                continue
            date = (datetime.fromisoformat(row["write_date"]) + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
            domains.append(["|", "&", ["id", "=", row["id"]], ["write_date", ">=", date], "&", ["id", "=", row["id"]], ["state", "!=", row.get("state", "draft")]])
        domains.append(["&", "&", ["id", "not in", ids], ["state", "=", "draft"], ["config_id", "in", [self.fixture["config_id"], *self.fixture.get("trusted_config_ids", [])]]])
        domain = ["|"] * (len(domains) - 1)
        for part in domains:
            domain.extend(part)
        record_ids = {model: list(self.cache[model]) for model in self.DYNAMIC if self.cache[model]}
        for model, order_field in (("pos.order.line", "order_id"), ("pos.payment", "pos_order_id")):
            record_ids[model] = [record_id for record_id, row in self.cache[model].items() if self.cache["pos.order"].get(row.get(order_field), {}).get("state", "draft") == "draft"]
        record_ids.setdefault("pos.order", [])
        record_ids["pos.order"] = ids
        return {"pos.order": domain}, record_ids

    async def follow_notifications(self):
        while True:
            message, received = await self.notifications.get()
            self.follow_active += 1
            try:
                notification_type = message.get("type", "")
                payload = message.get("payload") or {}
                if notification_type not in (self.fixture["access_token"] + "-SYNCHRONISATION", self.fixture["access_token"] + "-FLOOR_PLAN_UPD"):
                    continue
                if payload.get("session_id") != self.fixture["session_id"] or str(payload.get("device_identifier")) == str(self.identifier):
                    self.metrics.event("follower_skipped", device=self.name, notification_type=notification_type)
                    continue
                self.metrics.observe("follower_queue_lag", (time.monotonic_ns() - received) / 1e6)
                if notification_type.endswith("-FLOOR_PLAN_UPD"):
                    result = await self.call("pos.config", "get_floor_plan", [[self.fixture["config_id"]]])
                    self.merge(result.get("records", {}))
                else:
                    self.merge(payload.get("static_records", {}))
                    self.remove(payload.get("deleted_record_ids"))
                    domain, record_ids = self.open_orders_domain()
                    result = await self.call("pos.config", "read_config_open_orders", [[self.fixture["config_id"]], domain, record_ids])
                    await self.missing_recursive(result.get("dynamic_records", {}))
                    self.remove(result.get("deleted_record_ids"))
                    self.prune_closed()
                self.metrics.event("follower_complete", device=self.name, elapsed_ns=time.monotonic_ns() - received)
            except Exception as error:
                self.metrics.event("follower_error", device=self.name, error=str(error))
            finally:
                self.follow_active -= 1
                self.notifications.task_done()

    def prune_closed(self):
        # Frontend keeps current-session orders, but child lines/payments are
        # removed for completed orders. Bound retained history for long runs.
        closed = [key for key, row in self.cache["pos.order"].items() if row.get("state") != "draft"]
        keep = self.args.closed_cache
        for order_id in closed[:-keep] if keep else closed:
            self.cache["pos.order"].pop(order_id, None)
            for model, field in (("pos.order.line", "order_id"), ("pos.payment", "pos_order_id")):
                for key, row in list(self.cache[model].items()):
                    if row.get(field) == order_id:
                        del self.cache[model][key]

    async def close(self):
        self.closed = True
        tasks = [self.reader_task, self.heartbeat_task, *self.follow_tasks]
        for task in tasks:
            if task:
                task.cancel()
        await asyncio.gather(*(task for task in tasks if task), return_exceptions=True)
        if self.ws:
            await self.ws.close()
        if self.http:
            await self.http.close()
