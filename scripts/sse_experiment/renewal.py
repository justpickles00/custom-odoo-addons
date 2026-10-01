#!/usr/bin/env python3
"""Observe the unmodified SharedWorker through multiple five-minute leases."""
import argparse
import asyncio
import json
from pathlib import Path
import time
import uuid

import aiohttp

from run import Browser, authenticate, rpc


async def main(args):
    browser = Browser(9223)
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as session:
        await authenticate(session, args.url, "odoo_sse_exp_0000")
        cookie = next(c.value for c in session.cookie_jar if c.key == "session_id")
        browser.command("Network.setCookie", {"name": "session_id", "value": cookie, "url": args.url})
        browser.command("Page.navigate", {"url": args.url + "/sse_experiment"})
        for _ in range(100):
            if browser.evaluate("typeof window.subscribe==='function'"):
                break
            await asyncio.sleep(.1)
        fixture = (await rpc(session, args.url, "/sse_experiment/setup", {}))["result"]
        browser.evaluate(f"window.subscribe('sse', {json.dumps(fixture)}).then(()=>true)")
        identifiers = []
        started = time.perf_counter()
        for n in range(args.seconds):
            await asyncio.sleep(max(0, started + n - time.perf_counter()))
            identifier = uuid.uuid4().hex
            body = await rpc(session, args.url, "/sse_experiment/trial", {
                "stream": fixture["stream"], "probes": [], "mode": "sse", "duration_ms": 0,
                "count": 1, "run_id": identifier})
            identifiers.append({"id": identifier, "accepted": not bool(body.get("error")), "at_ms": time.time()*1000})
            if n % 30 == 0:
                print(f"SharedWorker renewal observation {n}/{args.seconds}s", flush=True)
                args.output.write_text(json.dumps({"offered": identifiers,
                    "received": browser.evaluate("window.samples"), "states": browser.evaluate("window.states")}, indent=2))
        await asyncio.sleep(2)
        result = {"offered": identifiers, "received": browser.evaluate("window.samples"),
                  "states": browser.evaluate("window.states")}
        args.output.write_text(json.dumps(result, indent=2))
        browser.evaluate("window.unsubscribe(); true")
        print(json.dumps({"offered":len(identifiers), "received":len(result["received"]),
                          "states":result["states"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--seconds", type=int, default=920)
    parser.add_argument("--output", type=Path, default=Path("/home/coder/playground/sse-utility-experiment/results/renewal.json"))
    asyncio.run(main(parser.parse_args()))
