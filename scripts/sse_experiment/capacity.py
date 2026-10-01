#!/usr/bin/env python3
"""Connection sweep/soak. Signs synthetic clients locally; no key is exported.

This tests transport capacity separately from login/authorization throughput.
All routed databases must already exist. Real browser renewal is tested by
the tracer separately; protocol clients renew synthetic tickets on EOF.
"""
import argparse
import asyncio
import base64
import hashlib
import hmac
import json
from pathlib import Path
import resource
import time
import uuid

import aiohttp
import orjson

from load import resources
from run import authenticate, rpc, summary, error_description


def signed_ticket(key, database, stream):
    data = base64.urlsafe_b64encode(json.dumps({"database": database, "uid": 2,
        "stream": stream, "scope": "isolated-capacity-fixture", "expires": int(time.time()) + 300}).encode()).decode()
    return data + "." + hmac.new(key, data.encode(), hashlib.sha256).hexdigest()


async def ipc_publish(path, database, stream, payload):
    reader, writer = await asyncio.open_unix_connection(str(path))
    try:
        writer.write((json.dumps({"database": database, "stream": stream, "payload": payload}, separators=(",", ":")) + "\n").encode())
        await writer.drain()
        return json.loads(await reader.readline())
    finally:
        writer.close()
        await writer.wait_closed()


async def window(args, session, cookies, service, count, seconds, repeat):
    channel = "sse.exp." + uuid.uuid4().hex
    key = (args.data / "sse/key").read_bytes()
    messages = {}
    expected = {}
    histogram = {}
    latency_count = 0
    latency_sum = 0
    latency_max = 0
    duplicates = 0

    def record(payload, index):
        nonlocal latency_count, latency_sum, latency_max, duplicates
        identifier = payload["event_id"]
        latency = (time.time_ns() - payload["sent_ns"]) / 1e6
        if args.histogram:
            if identifier not in expected:
                return
            bit = 1 << (index // len(args.databases))
            seen = messages.get(identifier, 0)
            if seen & bit:
                duplicates += 1
                return
            messages[identifier] = seen | bit
            bucket = int(latency * 10)
            histogram[bucket] = histogram.get(bucket, 0) + 1
            latency_count += 1
            latency_sum += latency
            latency_max = max(latency_max, latency)
        else:
            deliveries = messages.setdefault(identifier, {})
            if index in deliveries:
                duplicates += 1
            deliveries[index] = latency
    ready = 0
    connections = []
    tasks = []
    renewals = 0
    closed = 0
    connection_errors = []
    active = True
    def stream_for(index):
        return f"{channel}.{index}" if args.independent_streams else channel
    last_ids = {}
    warmup_retries = 0
    if service == "bus":
        # Normal Odoo clients start at the current bus id. Warm each gevent
        # registry before measuring steady transport capacity.
        for database in args.databases:
            for attempt in range(8):
                body = await rpc_with_cookie(session, args.bus, "/web/session/get_session_info", {}, cookies[database])
                if not body.get("error"):
                    break
                warmup_retries += 1
                await asyncio.sleep(min(3, .2 * 2**attempt))
            if body.get("error"):
                raise RuntimeError("Could not warm gevent tenant registry")
            last_ids[database] = body["result"]["bus_info"]["last_id"]
    async def consume_sse(index, database, response):
        nonlocal renewals, closed
        while active:
            try:
                buffer = b""
                while not response.content.at_eof():
                    buffer += await response.content.readany()
                    frames = buffer.split(b"\n\n")
                    buffer = frames.pop()
                    for raw in frames:
                        if raw.startswith(b"data: "):
                            event = orjson.loads(raw[6:])
                            if event.get("kind") == "event":
                                payload = event["payload"]
                                record(payload, index)
                if active:
                    renewals += 1
                    response = await session.get(args.sse + "/sse/stream", params={"ticket": signed_ticket(key, database, stream_for(index))})
                    connections[index] = response
                    if response.status != 200:
                        connection_errors.append(f"renewal status {response.status}")
                        break
            except Exception as error:
                if active:
                    closed += 1
                    connection_errors.append(error_description(error))
                break

    async def consume_bus(index, socket):
        nonlocal closed
        try:
            async for message in socket:
                if message.type == aiohttp.WSMsgType.TEXT:
                    data = orjson.loads(message.data)
                    if not isinstance(data, list):
                        continue
                    for item in data:
                        if item.get("message", {}).get("type") == "sse.experiment":
                            payload = item["message"]["payload"]
                            record(payload, index)
            if active:
                closed += 1
        except Exception as error:
            if active:
                connection_errors.append(error_description(error))

    sem = asyncio.Semaphore(1 if service == "bus" else 40)
    connections = [None] * count
    async def connect(index):
        nonlocal ready
        database = args.databases[index % len(args.databases)]
        async with sem:
            try:
                if service == "sse":
                    response = await session.get(args.sse + "/sse/stream", params={"ticket": signed_ticket(key, database, stream_for(index))})
                    if response.status != 200:
                        connection_errors.append(f"connect status {response.status}")
                        response.close()
                        return
                    # Confirm broker readiness rather than only HTTP headers.
                    await response.content.readline()
                    await response.content.readline()  # Consume the ready frame's blank line.
                    connections[index] = response
                    tasks.append(asyncio.create_task(consume_sse(index, database, response)))
                else:
                    socket = await session.ws_connect(args.bus + "/websocket?version=saas-19.5-1",
                        origin=args.http, headers={"Cookie": "session_id=" + cookies[database]})
                    await socket.send_json({"event_name": "subscribe", "data": {
                        "channels": [stream_for(index)], "last": last_ids[database], "check_outdated": False}})
                    connections[index] = socket
                    tasks.append(asyncio.create_task(consume_bus(index, socket)))
                    await asyncio.sleep(.003)
                ready += 1
            except Exception as error:
                connection_errors.append(f"{type(error).__name__}: status={getattr(error, 'status', None)}")
    connect_start = time.perf_counter()
    await asyncio.gather(*(connect(index) for index in range(count)))
    connect_s = time.perf_counter() - connect_start
    await asyncio.sleep(1)
    before = resources()
    calls = []
    publish_errors = []
    health = []
    async def ordinary_http():
        while active:
            started = time.perf_counter()
            try:
                body = await rpc_with_cookie(session, args.http, "/sse_experiment/health", {}, cookies[args.databases[0]])
                health.append({"ms": (time.perf_counter() - started) * 1000, "error": bool(body.get("error"))})
            except Exception:
                health.append({"ms": (time.perf_counter() - started) * 1000, "error": True})
            await asyncio.sleep(.2)
    health_task = asyncio.create_task(ordinary_http())
    async def send(n):
        target = n % count
        database = args.databases[target % len(args.databases)] if args.independent_streams else args.databases[n % len(args.databases)]
        stream = stream_for(target) if args.independent_streams else channel
        identifier = uuid.uuid4().hex
        # Endpoint adds :0. IPC uses the same identity for matching.
        event_id = identifier + ":0"
        expected[event_id] = int(connections[target] is not None) if args.independent_streams else sum(
            index % len(args.databases) == n % len(args.databases)
            for index in range(count) if connections[index] is not None)
        try:
            if service == "sse":
                await asyncio.wait_for(ipc_publish(args.data / "sse/publish.sock", database, stream,
                    {"event_id": event_id, "sent_ns": time.time_ns(), "kind": "load.event", "padding": "x" * 768}), 1)
            else:
                body = await rpc_with_cookie(session, args.http, "/sse_experiment/trial", {
                    "stream": stream, "probes": [], "mode": "bus", "count": 1,
                    "padding": 768, "duration_ms": 0, "run_id": identifier}, cookies[database])
                if body.get("error"):
                    publish_errors.append(body["error"]["message"])
        except Exception as error:
            publish_errors.append(error_description(error))
    started = time.perf_counter()
    for n in range(round(seconds * args.rate)):
        await asyncio.sleep(max(0, started + n / args.rate - time.perf_counter()))
        calls.append(asyncio.create_task(send(n)))
    await asyncio.gather(*calls)
    offered_elapsed = time.perf_counter() - started
    await asyncio.sleep(1)
    after = resources()
    active = False
    health_task.cancel()
    await asyncio.gather(health_task, return_exceptions=True)
    if args.histogram:
        def quantile(p):
            cumulative = 0
            for bucket, n in sorted(histogram.items()):
                cumulative += n
                if cumulative > (latency_count - 1) * p:
                    return (bucket + .5) / 10
        latency_stats = {"n": latency_count, "mean": latency_sum / latency_count if latency_count else None,
                         "p50": quantile(.5), "p95": quantile(.95), "p99": quantile(.99),
                         "max": latency_max, "histogram_bin_ms": .1}
        received_count = latency_count
    else:
        latencies = [v for identifier in expected for v in messages.get(identifier, {}).values()]
        latency_stats = summary(latencies)
        received_count = len(latencies)
    expected_count = sum(expected.values())
    result = {"service": service, "connections": count, "ready": ready,
        "databases": len(args.databases), "repeat": repeat, "seconds": seconds,
        "independent_streams": args.independent_streams,
        "warmup_retries": warmup_retries,
        "rate": args.rate, "connect_s": connect_s, "send_elapsed_s": offered_elapsed,
        "offered_events": len(expected), "expected_deliveries": expected_count,
        "received_deliveries": received_count, "delivery_fraction": received_count/expected_count if expected_count else 0,
        "duplicate_deliveries": duplicates,
        "latency_ms": latency_stats, "http_ms": summary([v["ms"] for v in health]),
        "http_errors": sum(v["error"] for v in health), "renewals": renewals,
        "unexpected_closes": closed, "connection_errors": connection_errors,
        "publish_errors": publish_errors, "resources_before": before, "resources_after": after}
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    close_sem = asyncio.Semaphore(4)
    async def close(connection):
        if connection:
            if service == "sse":
                connection.close()
            else:
                async with close_sem:
                    await connection.close()
                    await asyncio.sleep(.01)
    await asyncio.gather(*(close(connection) for connection in connections))
    if service == "bus":
        await asyncio.sleep(5)
    if service == "sse":
        # Clear disconnected subscribers immediately on the next write.
        cleanup_sem = asyncio.Semaphore(40)
        async def cleanup(index):
            database = args.databases[index % len(args.databases)]
            stream = stream_for(index) if args.independent_streams else channel
            async with cleanup_sem:
                await ipc_publish(args.data / "sse/publish.sock", database, stream,
                                  {"event_id": "cleanup", "sent_ns": time.time_ns()})
        for _ in range(3):
            await asyncio.gather(*(cleanup(index) for index in range(count if args.independent_streams else len(args.databases))))
            await asyncio.sleep(.1)
    print(json.dumps({k:v for k,v in result.items() if not k.startswith("resources")}), flush=True)
    return result


async def rpc_with_cookie(session, url, path, params, cookie):
    async with session.post(url + path, headers={"Cookie": "session_id=" + cookie},
        json={"jsonrpc": "2.0", "method": "call", "id": 1, "params": params}) as response:
        if response.status != 200:
            return {"error": {"message": f"HTTP {response.status}"}}
        return await response.json()


async def main(args):
    resource.setrlimit(resource.RLIMIT_NOFILE, (65536, resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
    args.databases = [f"odoo_sse_exp_{index:04d}" for index in range(args.tenants)]
    results = json.loads(args.output.read_text()) if args.resume and args.output.exists() else []
    cookies = {}
    async with aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar(),
        connector=aiohttp.TCPConnector(limit=0), timeout=aiohttp.ClientTimeout(total=None, sock_connect=15)) as session:
        for database in args.databases:
            async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as auth_session:
                await authenticate(auth_session, args.http, database)
                cookies[database] = next(c.value for c in auth_session.cookie_jar if c.key == "session_id")
        for count in args.counts:
            for repeat in range(args.repeats):
                services = ["bus", "sse"] if repeat % 2 == 0 else ["sse", "bus"]
                if args.service:
                    services = [args.service]
                for service in services:
                    if any(r["service"] == service and r["connections"] == count and
                           r["repeat"] == repeat and r.get("independent_streams", False) == args.independent_streams for r in results):
                        continue
                    results.append(await window(args, session, cookies, service, count, args.seconds, repeat))
                    args.output.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", default="http://127.0.0.1:8076")
    parser.add_argument("--bus", default="http://127.0.0.1:8077")
    parser.add_argument("--sse", default="http://127.0.0.1:8078")
    parser.add_argument("--data", type=Path, default=Path("/home/coder/playground/sse-utility-experiment/data"))
    parser.add_argument("--tenants", type=int, default=32)
    parser.add_argument("--counts", type=int, nargs="+", default=[256,512,1024,2048,4096,8192])
    parser.add_argument("--rate", type=float, default=200)
    parser.add_argument("--seconds", type=float, default=15)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--service", choices=["sse", "bus"])
    parser.add_argument("--histogram", action="store_true", help="Bound latency storage during long soaks; quantiles have 0.1ms resolution")
    parser.add_argument("--independent-streams", action="store_true", help="One agent stream per connection rather than database-wide broadcast")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("/home/coder/playground/sse-utility-experiment/results/capacity.json"))
    asyncio.run(main(parser.parse_args()))
