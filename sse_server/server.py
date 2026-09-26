import asyncio
from collections import Counter, defaultdict
import json
import logging
import os
import signal
import time
import uuid

from aiohttp import web

from odoo import sql_db
from odoo.service.server import PreforkServer, Worker, pipe_ping

from .api import MAX_MESSAGE, runtime_dir, secret, verify_ticket

_logger = logging.getLogger(__name__)


class Broker:
    def __init__(self):
        self.epoch = uuid.uuid4().hex
        self.sequence = 0
        self.subscribers = defaultdict(dict)
        self.connections = Counter()

    def emit(self, database, stream, payload):
        self.sequence += 1
        stamp = {"epoch": self.epoch, "seq": self.sequence}
        message = {"kind": "event", **stamp, "payload": payload}
        for queue, task in tuple(self.subscribers.get((database, stream), {}).items()):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                task.cancel()  # Reconnect and obtain a fresh snapshot.
        return stamp

    async def receive(self, reader, writer):
        try:
            async with asyncio.timeout(2):
                raw = await reader.readline()
                if not raw or len(raw) > MAX_MESSAGE:
                    return
                message = json.loads(raw)
                stamp = self.emit(message["database"], message["stream"], message["payload"])
                writer.write(json.dumps(stamp).encode() + b"\n")
                await writer.drain()
        except (ValueError, KeyError, OSError, TimeoutError):
            _logger.debug("Discarded incomplete SSE publication", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()

    async def stream(self, request):
        try:
            claims = verify_ticket(request.query.get("ticket", ""))
        except (ValueError, KeyError, TypeError):
            raise web.HTTPUnauthorized() from None
        database = claims["database"]
        if self.connections[database] >= 128 or self.connections.total() >= 1024:
            raise web.HTTPServiceUnavailable()
        key = (database, claims["stream"])
        queue = asyncio.Queue(maxsize=100)
        self.subscribers[key][queue] = asyncio.current_task()
        self.connections[database] += 1
        response = web.StreamResponse(headers={
            "Content-Type": "text/event-stream", "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        })
        try:
            await response.prepare(request)
            async with asyncio.timeout(claims["expires"] - time.time()):
                await response.write(self.frame({"kind": "ready", "epoch": self.epoch}))
                while True:
                    try:
                        message = await asyncio.wait_for(queue.get(), 15)
                        frame = self.frame(message)
                    except TimeoutError:
                        frame = b": heartbeat\n\n"
                    async with asyncio.timeout(5):
                        await response.write(frame)
        except (ConnectionError, TimeoutError):
            pass
        finally:
            self.subscribers[key].pop(queue, None)
            if not self.subscribers[key]:
                del self.subscribers[key]
            self.connections[database] -= 1
            if not self.connections[database]:
                del self.connections[database]
        return response

    @staticmethod
    def frame(message):
        return f"data: {json.dumps(message, separators=(',', ':'))}\n\n".encode()


class SSEWorker(Worker):
    def run(self):
        self.start()
        signal.signal(signal.SIGTERM, self.signal_handler)
        if self.multi.socket:
            self.multi.socket.close()
        asyncio.run(self.serve())

    async def serve(self):
        broker = Broker()
        path = runtime_dir() / "publish.sock"
        path.unlink(missing_ok=True)
        ipc = await asyncio.start_unix_server(broker.receive, path=str(path), limit=MAX_MESSAGE)
        os.chmod(path, 0o600)
        app = web.Application()
        app.router.add_get("/sse/stream", broker.stream)
        runner = web.AppRunner(app, access_log=None, shutdown_timeout=1)
        await runner.setup()
        port = int(os.environ.get("ODOO_SSE_PORT", "8073"))
        try:
            await web.TCPSite(runner, "127.0.0.1", port).start()
            self.logger.info("SSE server listening on 127.0.0.1:%s", port)
            while self.alive:
                self.check_limits()
                pipe_ping(self.watchdog_pipe)
                await asyncio.sleep(1)
        finally:
            ipc.close()
            await ipc.wait_closed()
            await runner.cleanup()
            path.unlink(missing_ok=True)


def post_load():
    if getattr(PreforkServer.process_spawn, "_sse", False):
        # JCB: If this happen, perhaps it's better to raise an error
        return
    original = PreforkServer.process_spawn

    def spawn(server):
        original(server)
        if not any(isinstance(worker, SSEWorker) for worker in server.workers.values()):
            secret()  # Create once, before forking the SSE child.
            sql_db.close_all()
            server.worker_spawn(SSEWorker, {})

    spawn._sse = True
    PreforkServer.process_spawn = spawn
