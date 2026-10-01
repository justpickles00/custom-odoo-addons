#!/usr/bin/env python3
"""Separate rare synchronous-wait profile during the mixed-service soak."""
import asyncio
import json
from pathlib import Path
import time

import aiohttp

from run import authenticate, rpc, summary


async def main():
    url = "http://127.0.0.1:8080"
    results = []
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as session:
        await authenticate(session, url, "odoo_sse_exp_0000")
        for occupied in [0, 1, 2]:
            fixtures = [(await rpc(session, url, "/sse_experiment/setup", {}))["result"] for _ in range(occupied)]
            holds = [asyncio.create_task(rpc(session, url, "/sse_experiment/trial", {
                "stream":f["stream"], "probes":f["probes"], "mode":"sse", "duration_ms":10000})) for f in fixtures]
            await asyncio.sleep(.05)
            latencies = []
            errors = []
            workers = set()
            async def health():
                before = time.perf_counter()
                result = await rpc(session, url, "/sse_experiment/health", {})
                latencies.append((time.perf_counter()-before)*1000)
                if result.get("error"):
                    errors.append(result["error"]["message"])
                else:
                    workers.add(result["result"]["pid"])
            start = time.perf_counter()
            calls = []
            for n in range(500):
                await asyncio.sleep(max(0,start+n/50-time.perf_counter()))
                calls.append(asyncio.create_task(health()))
            await asyncio.gather(*calls)
            outcomes = await asyncio.gather(*holds)
            results.append({"occupied_workers":occupied,"wait_ms":10000,"offered_http_rate":50,
                "http_ms":summary(latencies),"errors":errors,"http_workers_seen":sorted(workers),
                "held_request_success":all(not o.get("error") for o in outcomes)})
            print(json.dumps(results[-1]),flush=True)
    Path("/home/coder/playground/sse-utility-experiment/results/occupancy.json").write_text(json.dumps(results,indent=2))


if __name__ == "__main__":
    asyncio.run(main())
