"""Test-only replacement of the two SSE admission constants.

Absent SSE_EXPERIMENT_GLOBAL_CAP, this hook does nothing. The experiment's
server-wide module list must explicitly include this addon to apply the override.
No queue, timeout, payload, framing, or ticket behavior is changed.
"""
import inspect
import os
import textwrap


def post_load():
    if "SSE_EXPERIMENT_CRON_FLEET" in os.environ:
        from odoo.service import server

        size = int(os.environ["SSE_EXPERIMENT_CRON_FLEET"])
        if not 1 <= size <= 1000:
            raise ValueError("Invalid isolated cron fleet")
        # Select only experiment-owned DBs, without preloading 1,000 registries
        # into the supervisor or touching other projects' databases on this VM.
        server.cron_database_list = lambda: [f"odoo_sse_exp_{index:04d}" for index in range(size)]
    if "SSE_EXPERIMENT_GLOBAL_CAP" not in os.environ:
        return
    from odoo.addons.sse_server.server import Broker

    total = int(os.environ["SSE_EXPERIMENT_GLOBAL_CAP"])
    tenant = int(os.environ["SSE_EXPERIMENT_TENANT_CAP"])
    if not 1 <= tenant <= total <= 65536:
        raise ValueError("Invalid isolated experiment admission caps")
    source = textwrap.dedent(inspect.getsource(Broker.stream))
    guard = "self.connections[database] >= 128 or self.connections.total() >= 1024"
    if source.count(guard) != 1:
        raise RuntimeError("Admission guard changed; review the experiment override")
    source = source.replace(guard, f"self.connections[database] >= {tenant} or self.connections.total() >= {total}")
    namespace = {}
    exec(compile(source, "<SSE experiment admission override>", "exec"), Broker.stream.__globals__, namespace)
    Broker.stream = namespace["stream"]
