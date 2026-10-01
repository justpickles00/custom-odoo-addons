#!/usr/bin/env python3
"""Run against an isolated, already initialized prefork Odoo deployment.

Python dependencies: aiohttp, websocket-client, psycopg2, psutil.
Headless Chrome must expose its DevTools endpoint on --chrome-port.
"""
import argparse
import asyncio
from contextlib import closing
import json
from pathlib import Path
import random
import statistics
import time
import urllib.request
import uuid

import aiohttp
import psycopg2
import websocket


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * p))]


def error_description(error):
    """Keep request cookies and ticket URLs out of saved diagnostics."""
    result = type(error).__name__
    if getattr(error, "status", None):
        result += f": HTTP {error.status}"
    if isinstance(error, OSError) and error.errno:
        result += f": errno {error.errno}"
    return result


def summary(values):
    return {"n": len(values), "mean": statistics.mean(values) if values else None,
            "p50": percentile(values, .5), "p95": percentile(values, .95),
            "p99": percentile(values, .99), "max": max(values) if values else None}


class Browser:
    def __init__(self, port):
        self.port = port
        targets = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/json"))
        self.target = next(t for t in targets if t["type"] == "page")

    def command(self, method, params=None):
        with closing(websocket.create_connection(
                self.target["webSocketDebuggerUrl"], origin=f"http://127.0.0.1:{self.port}", timeout=30)) as sock:
            sock.send(json.dumps({"id": 1, "method": method, "params": params or {}}))
            while True:
                response = json.loads(sock.recv())
                if response.get("id") == 1:
                    if "error" in response:
                        raise RuntimeError(response)
                    return response.get("result")

    def evaluate(self, expression):
        result = self.command("Runtime.evaluate", {"expression": expression,
            "awaitPromise": True, "returnByValue": True})
        if result.get("exceptionDetails"):
            raise RuntimeError(result)
        return result["result"].get("value")


async def rpc(session, url, path, params):
    async with session.post(url + path, json={"jsonrpc": "2.0", "method": "call", "id": 1,
                                            "params": params}) as response:
        body = await response.json()
        return body


async def authenticate(session, url, database):
    body = await rpc(session, url, "/web/session/authenticate",
                     {"db": database, "login": "admin", "password": "admin"})
    if body.get("error"):
        raise RuntimeError(body)
    return body["result"]


async def latency(args):
    browser = Browser(args.chrome_port)
    observer = psycopg2.connect(dbname=args.database, user="coder", host="/var/run/postgresql")
    observer.autocommit = True
    rows = []
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as session:
        await authenticate(session, args.url, args.database)
        cookie = next(c.value for c in session.cookie_jar if c.key == "session_id")
        browser.command("Network.setCookie", {"name": "session_id", "value": cookie, "url": args.url})
        browser.command("Page.navigate", {"url": args.url + "/sse_experiment"})
        for _ in range(100):
            if browser.evaluate("typeof window.subscribe === 'function'"):
                break
            await asyncio.sleep(.1)
        # Warm all available HTTP workers; record their effective registry cap.
        warm = await asyncio.gather(*(rpc(session, args.url, "/sse_experiment/health", {}) for _ in range(80)))
        (args.output / "workers.json").write_text(json.dumps(warm, indent=2))
        fixtures = {}
        for mode in ["bus", "bus_separate", "sse"]:
            fixtures[mode] = (await rpc(session, args.url, "/sse_experiment/setup", {}))["result"]

        distribution = [50] * 4 + [100] * 8 + [200] * 6 + [300] + [500]
        rng = random.Random(20261001)
        trials = [(duration, failed) for failed in [False, True]
                  for duration in distribution * args.repeats]
        rng.shuffle(trials)
        # Rotate adapters per trial to reduce clock/order bias.
        for trial_index, (duration, failed) in enumerate(trials + [(5000, False), (5000, True)]):
            modes = ["bus", "bus_separate", "sse"]
            modes = modes[trial_index % 3:] + modes[:trial_index % 3]
            for mode in modes:
                fixture = fixtures[mode]
                browser.evaluate("window.unsubscribe(); true")
                browser.evaluate(f"window.subscribe({json.dumps(mode)}, {json.dumps(fixture)}).then(()=>true)")
                with observer.cursor() as cr:
                    cr.execute("UPDATE sse_experiment_probe SET value=0 WHERE id=ANY(%s)", (fixture["probes"],))
                identifier = uuid.uuid4().hex
                params = {**{k: fixture[k] for k in ["stream", "probes"]}, "mode": mode,
                          "duration_ms": duration, "fail": failed, "run_id": identifier}
                task = asyncio.create_task(rpc(session, args.url, "/sse_experiment/trial", params))
                await asyncio.sleep(duration / 2000)
                during_samples = browser.evaluate("window.samples")
                with observer.cursor() as cr:
                    cr.execute("SELECT value FROM sse_experiment_probe WHERE id=ANY(%s) ORDER BY id", (fixture["probes"],))
                    during_values = [v[0] for v in cr.fetchall()]
                response = await task
                await asyncio.sleep(.12)
                samples = browser.evaluate("window.samples")
                samples = [s for s in samples if s["run_id"] == identifier]
                with observer.cursor() as cr:
                    cr.execute("SELECT value FROM sse_experiment_probe WHERE id=ANY(%s) ORDER BY id", (fixture["probes"],))
                    after_values = [v[0] for v in cr.fetchall()]
                audit = [json.loads(line) for line in args.audit.read_text().splitlines()]
                audit = [a for a in audit if a["run_id"] == identifier]
                end = next(a for a in audit if a["outcome"] == "request_end")
                outcome = next(a for a in audit if a["outcome"] in {"commit", "rollback"})
                first = min(samples, key=lambda s: s["sequence"]) if samples else None
                rows.append({"mode": mode, "duration_ms": duration, "fail": failed, "run_id": identifier,
                             "during_values": during_values, "after_values": after_values,
                             "events_during": len([s for s in during_samples if s["run_id"] == identifier]),
                             "events_received": len(samples), "expected_events": 4,
                             "first_dom_ms": (first["rendered_ns"] - first["sent_ns"]) / 1e6 if first else None,
                             "lead_before_outcome_ms": (outcome["at_ns"] - first["rendered_ns"]) / 1e6 if first else None,
                             "request_ms": (end["at_ns"] - end["started_ns"]) / 1e6,
                             "cost_ms": end["cost_ms"], "outcome": outcome["outcome"],
                             "http_error": bool(response.get("error")), "samples": samples})
                (args.output / "latency.json").write_text(json.dumps(rows, indent=2))
            print(f"latency {trial_index + 1}/{len(trials) + 2} duration={duration} fail={failed}", flush=True)
        browser.evaluate("window.unsubscribe(); true")
    observer.close()
    result = {}
    for mode in ["bus", "bus_separate", "sse"]:
        regular = [r for r in rows if r["mode"] == mode and r["duration_ms"] < 1000]
        success = [r for r in regular if not r["fail"]]
        failure = [r for r in regular if r["fail"]]
        result[mode] = {"first_dom_ms": summary([r["first_dom_ms"] for r in success]),
                        "publish_cost_ms": summary([c for r in regular for c in r["cost_ms"]]),
                        "request_ms": summary([r["request_ms"] for r in regular]),
                        "success_received": sum(r["events_received"] for r in success),
                        "failure_received": sum(r["events_received"] for r in failure),
                        "rollback_correct": all(r["after_values"] == [0, 0] for r in failure),
                        "uncommitted_hidden": all(r["during_values"] == [0, 0] for r in regular)}
    (args.output / "latency-summary.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["latency"])
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--database", default="odoo_sse_exp_0000")
    parser.add_argument("--chrome-port", type=int, default=9223)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--output", type=Path, default=Path("/home/coder/playground/sse-utility-experiment/results"))
    parser.add_argument("--audit", type=Path, default=Path("/home/coder/playground/sse-utility-experiment/data/experiment.jsonl"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    asyncio.run(latency(args))


if __name__ == "__main__":
    main()
