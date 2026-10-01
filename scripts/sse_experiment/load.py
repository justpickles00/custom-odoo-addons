#!/usr/bin/env python3
"""Fixed offered-rate fanout benchmark of actual Odoo endpoints."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import time
import uuid

import aiohttp
import psutil

from run import authenticate, rpc, summary, error_description


def resources():
    result = []
    for process in psutil.process_iter(["pid", "ppid", "cmdline"]):
        command = " ".join(process.info["cmdline"] or [])
        if ("odoo-bin" in command and "/sse-utility-experiment/odoo.conf" in command) or process.pid == os.getpid():
            try:
                full = process.memory_full_info()
                result.append({"pid": process.pid, "ppid": process.ppid(),
                               "rss": full.rss, "pss": full.pss,
                               "fds": process.num_fds(),
                               "cpu_s": sum(process.cpu_times()[:2])})
            except psutil.Error:
                pass
    return result


async def run_window(args, session, fixture, mode, subscribers, rate, padding, repeat):
    received = {}
    errors = []
    sockets = []
    tasks = []
    async def sse_consume(response, index):
        async for raw in response.content:
            if raw.startswith(b"data: "):
                message = json.loads(raw[6:])
                if message.get("kind") == "event":
                    payload = message["payload"]
                    received.setdefault(payload["event_id"], {})[index] = (
                        time.time_ns() - payload["sent_ns"]) / 1e6

    async def bus_consume(socket, index):
        async for message in socket:
            if message.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(message.data)
                if not isinstance(data, list):
                    continue
                for item in data:
                    if item.get("message", {}).get("type") == "sse.experiment":
                        payload = item["message"]["payload"]
                        received.setdefault(payload["event_id"], {})[index] = (
                            time.time_ns() - payload["sent_ns"]) / 1e6
    if mode == "sse":
        ticket = (await rpc(session, args.url, "/sse/connect", {"stream": fixture["stream"]}))["result"]
        for index in range(subscribers):
            response = await session.get(args.url + ticket["url"])
            if response.status != 200:
                raise RuntimeError(f"SSE connection rejected: {response.status}")
            sockets.append(response)
            tasks.append(asyncio.create_task(sse_consume(response, index)))
    else:
        for index in range(subscribers):
            socket = await session.ws_connect(args.url + "/websocket?version=" + fixture["version"], origin=args.url)
            await socket.send_json({"event_name": "subscribe", "data": {
                "channels": [fixture["stream"]], "last": 0, "check_outdated": False}})
            sockets.append(socket)
            tasks.append(asyncio.create_task(bus_consume(socket, index)))
    await asyncio.sleep(.4)
    initial = resources()
    calls = []
    costs = []
    request_times = []
    identifiers = []
    sem = asyncio.Semaphore(20)
    max_inflight = 0
    inflight = 0
    async def send(identifier):
        nonlocal inflight, max_inflight
        inflight += 1
        max_inflight = max(max_inflight, inflight)
        started = time.perf_counter()
        try:
            async with sem:
                response = await rpc(session, args.url, "/sse_experiment/trial", {
                    "stream": fixture["stream"], "probes": [], "mode": mode,
                    "duration_ms": 0, "count": 1, "padding": padding, "run_id": identifier})
                if "error" in response:
                    errors.append(response["error"]["message"])
                else:
                    costs.extend(response["result"]["cost_ms"])
        except Exception as error:
            errors.append(error_description(error))
        finally:
            request_times.append((time.perf_counter() - started) * 1000)
            inflight -= 1
    start = time.perf_counter()
    for n in range(round(args.seconds * rate)):
        await asyncio.sleep(max(0, start + n / rate - time.perf_counter()))
        identifier = uuid.uuid4().hex
        identifiers.append(identifier + ":0")
        calls.append(asyncio.create_task(send(identifier)))
    await asyncio.gather(*calls)
    send_elapsed = time.perf_counter() - start
    await asyncio.sleep(.6)
    final = resources()
    latencies = [latency for identifier in identifiers for latency in received.get(identifier, {}).values()]
    expected = len(identifiers) * subscribers
    result = {"mode": mode, "subscribers": subscribers, "rate": rate, "padding": padding,
              "repeat": repeat, "seconds": args.seconds, "offered_events": len(identifiers),
              "expected_deliveries": expected, "received_deliveries": len(latencies),
              "delivery_fraction": len(latencies) / expected, "latency_ms": summary(latencies),
              "publish_cost_ms": summary(costs), "http_ms": summary(request_times),
              "send_elapsed_s": send_elapsed, "max_inflight": max_inflight, "errors": errors,
              "resources_before": initial, "resources_after": final}
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for socket in sockets:
        if mode == "sse":
            socket.close()
        else:
            await socket.close()
    if mode != "sse":
        # Let server-side close callbacks release transient database cursors
        # before the next connection ramp begins.
        await asyncio.sleep(1)
    # SSE detects a disconnect on the next write, not when the client closes.
    # A final publication retires disconnected queues before the next window.
    if mode == "sse":
        for _ in range(5):
            await rpc(session, args.url, "/sse_experiment/trial", {"stream": fixture["stream"],
                      "probes": [], "mode": "sse", "duration_ms": 0, "count": 1})
            await asyncio.sleep(.1)
    print(json.dumps({k:v for k,v in result.items() if not k.startswith("resources")}), flush=True)
    return result


async def main(args):
    results = json.loads(args.output.read_text()) if args.resume and args.output.exists() else []
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True),
            connector=aiohttp.TCPConnector(limit=0), timeout=aiohttp.ClientTimeout(total=None)) as session:
        await authenticate(session, args.url, args.database)
        fixture = (await rpc(session, args.url, "/sse_experiment/setup", {}))["result"]
        cases = [(n, rate, 768) for n in [1, 96] for rate in [50, 200]]
        if args.payloads:
            cases = [(96, 20, padding) for padding in [16384, 65536, 240000]]
        for subscribers, rate, padding in cases:
            for repeat in range(args.repeats):
                modes = ["bus", "bus_separate", "sse"]
                modes = modes[repeat % 3:] + modes[:repeat % 3]
                for mode in modes:
                    if any(r["mode"] == mode and r["subscribers"] == subscribers and
                           r["rate"] == rate and r["padding"] == padding and r["repeat"] == repeat for r in results):
                        continue
                    # Unique channel prevents retained bus history contaminating a new window.
                    fixture = (await rpc(session, args.url, "/sse_experiment/setup", {}))["result"]
                    results.append(await run_window(args, session, fixture, mode, subscribers, rate, padding, repeat))
                    args.output.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--database", default="odoo_sse_exp_0000")
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--payloads", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("/home/coder/playground/sse-utility-experiment/results/load.json"))
    asyncio.run(main(parser.parse_args()))
