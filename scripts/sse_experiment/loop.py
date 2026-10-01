#!/usr/bin/env python3
"""Three fake model round trips, posting responses to real HTTP tool workers."""
import asyncio
import json
from pathlib import Path
import time
import uuid

from aiohttp import web, ClientSession, CookieJar
import psycopg2

from run import authenticate, Browser, rpc


async def main():
    completed = {}
    provider_tasks = []
    url = "http://127.0.0.1:8080"
    async with ClientSession(cookie_jar=CookieJar(unsafe=True)) as session:
        await authenticate(session, url, "odoo_sse_exp_0000")
        async def model(request):
            body = await request.json()
            params = body["params"]
            async def callback():
                await asyncio.sleep(2)
                params["event_offset"] += 1
                params["model_response"] = True
                started = time.time_ns()
                async with session.post(url + "/sse_experiment/trial",
                    headers={"Cookie": "session_id=" + body["cookie"]},
                    json={"jsonrpc":"2.0", "id":1,"method":"call","params":params}) as response:
                    result = await response.json()
                completed[params["event_offset"]] = {"result": result, "started_ns": started, "end_ns": time.time_ns()}
            provider_tasks.append(asyncio.create_task(callback()))
            return web.json_response({"queued": True}, status=202)
        app = web.Application()
        app.router.add_post("/model", model)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 8081).start()
        browser = Browser(9223)
        cookie = next(c.value for c in session.cookie_jar if c.key == "session_id")
        browser.command("Network.setCookie", {"name":"session_id","value":cookie,"url":url})
        browser.command("Page.navigate", {"url":url + "/sse_experiment"})
        for _ in range(50):
            if browser.evaluate("typeof window.subscribe==='function'"):
                break
            await asyncio.sleep(.1)
        observer = psycopg2.connect(dbname="odoo_sse_exp_0000", user="coder", host="/var/run/postgresql")
        observer.autocommit = True
        results = []
        for mode in ["bus", "bus_separate", "sse"]:
            completed.clear()
            fixtures = [(await rpc(session, url, "/sse_experiment/setup", {}))["result"] for _ in range(3)]
            stream = fixtures[0]["stream"]
            browser.evaluate("window.unsubscribe(); true")
            browser.evaluate(f"window.subscribe({json.dumps(mode)},{json.dumps(fixtures[0])}).then(()=>true)")
            run_id = uuid.uuid4().hex
            rounds = []
            for index, fixture in enumerate(fixtures):
                params = {"stream":stream,"probes":fixture["probes"],"mode":mode,
                          "duration_ms":150,"fail":index == 2,"run_id":run_id,
                          "event_offset":index * 6,"count":4}
                started = time.perf_counter()
                response = await rpc(session, url, "/sse_experiment/request_model", {"params":params})
                request_ms = (time.perf_counter() - started) * 1000
                while index * 6 + 1 not in completed:
                    await asyncio.sleep(.01)
                rounds.append({"request_ms":request_ms, "queued":response,
                               "callback":completed[index * 6 + 1]})
            await asyncio.sleep(.15)
            with observer.cursor() as cr:
                cr.execute("SELECT id,value FROM sse_experiment_probe WHERE id=ANY(%s) ORDER BY id",
                           ([identifier for f in fixtures for identifier in f["probes"]],))
                values = cr.fetchall()
            results.append({"mode":mode,"run_id":run_id,"rounds":rounds,"probes":values,
                            "events":browser.evaluate("window.samples")})
            print(mode, "events",len(results[-1]["events"]),"probes",values, flush=True)
        Path("/home/coder/playground/sse-utility-experiment/results/loop.json").write_text(json.dumps(results, indent=2))
        browser.evaluate("window.unsubscribe(); true")
        observer.close()
        await asyncio.gather(*provider_tasks)
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
