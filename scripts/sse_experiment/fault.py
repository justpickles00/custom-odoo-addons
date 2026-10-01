#!/usr/bin/env python3
"""Pause ONLY the isolated test SSE child to check publication failure policy."""
import asyncio
import json
import os
from pathlib import Path
import signal
import time

import aiohttp
import psutil
import psycopg2

from run import authenticate, rpc


async def main():
    listener = next(c for c in psutil.net_connections(kind="tcp") if c.laddr.port == 8078 and c.status == "LISTEN")
    process = psutil.Process(listener.pid)
    assert "/home/coder/playground/sse-utility-experiment/odoo.conf" in process.cmdline()
    rows = []
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as session:
        await authenticate(session, "http://127.0.0.1:8080", "odoo_sse_exp_0000")
        fixtures = [(await rpc(session, "http://127.0.0.1:8080", "/sse_experiment/setup", {}))["result"] for _ in range(2)]
        os.kill(process.pid, signal.SIGSTOP)
        try:
            for mode, fixture in zip(["bus", "sse"], fixtures):
                before = time.perf_counter()
                response = await asyncio.wait_for(rpc(session, "http://127.0.0.1:8080", "/sse_experiment/trial", {
                    "stream":fixture["stream"], "probes":fixture["probes"], "mode":mode,"duration_ms":150}), 5)
                rows.append({"mode":mode,"elapsed_ms":(time.perf_counter()-before)*1000,
                             "error_type":response.get("error",{}).get("data",{}).get("name"),
                             "request_failed":bool(response.get("error")),"probes":fixture["probes"]})
        finally:
            os.kill(process.pid, signal.SIGCONT)
        observer = psycopg2.connect(dbname="odoo_sse_exp_0000",user="coder",host="/var/run/postgresql")
        with observer.cursor() as cr:
            for row in rows:
                cr.execute("SELECT value FROM sse_experiment_probe WHERE id=ANY(%s) ORDER BY id",(row["probes"],))
                row["values"]=[v[0] for v in cr.fetchall()]
        observer.close()
    Path("/home/coder/playground/sse-utility-experiment/results/fault.json").write_text(json.dumps(rows,indent=2))
    print(json.dumps(rows),flush=True)


if __name__ == "__main__":
    asyncio.run(main())
