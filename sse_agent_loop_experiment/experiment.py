"""Administrator-only synthetic experiment; never install in production."""
import json
import os
import time
import uuid
import urllib.request
from pathlib import Path

from odoo import api, fields, http, models, SUPERUSER_ID
from odoo.exceptions import AccessError, ValidationError
from odoo.http import request
from odoo.modules.registry import Registry
from odoo.tools import config
from odoo.addons.sse_server.api import publish


class Probe(models.Model):
    _name = "sse.experiment.probe"
    _description = "Isolated transaction probe"

    name = fields.Char(required=True)
    value = fields.Integer(default=0)


class StreamAccess(models.AbstractModel):
    _inherit = "sse.stream"

    def _authorize(self, stream):
        if stream.startswith("sse.exp.") and self.env.user.has_group("base.group_system"):
            return
        return super()._authorize(stream)


def audit(record):
    path = Path(config["data_dir"]) / "experiment.jsonl"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, (json.dumps(record, separators=(",", ":")) + "\n").encode())
    finally:
        os.close(fd)


class Experiment(http.Controller):
    def admin(self):
        if not request.env.user.has_group("base.group_system"):
            raise AccessError("Experiment requires an administrator.")

    @http.route("/sse_experiment/setup", type="jsonrpc", auth="user")
    def setup(self):
        self.admin()
        identifier = uuid.uuid4().hex
        probes = request.env["sse.experiment.probe"].sudo().create([
            {"name": identifier + ":a"}, {"name": identifier + ":b"},
        ])
        return {"stream": "sse.exp." + identifier, "probes": probes.ids,
                "version": "saas-19.5-1"}

    @http.route("/sse_experiment/trial", type="jsonrpc", auth="user")
    def trial(self, stream, probes, mode, duration_ms=150, fail=False,
              count=4, padding=0, run_id=None, channels=None, event_offset=0,
              event_kind="load.event", model_response=False):
        self.admin()
        if mode not in {"bus", "bus_separate", "sse"}:
            raise ValidationError("Invalid adapter")
        if not stream.startswith("sse.exp.") or not 0 <= duration_ms <= 10000:
            raise ValidationError("Invalid experiment parameters")
        if not 1 <= count <= 1000 or not 0 <= padding <= 240000:
            raise ValidationError("Invalid event workload")
        db = request.env.cr.dbname
        identifier = run_id or uuid.uuid4().hex
        started = time.time_ns()
        sequence = int(event_offset)
        costs = []

        def outcome(value):
            audit({"run_id": identifier, "outcome": value, "at_ns": time.time_ns(),
                   "db": db, "pid": os.getpid()})

        request.env.cr.postcommit.add(lambda: outcome("commit"))
        request.env.cr.postrollback.add(lambda: outcome("rollback"))

        def emit(kind):
            nonlocal sequence
            payload = {"run_id": identifier, "event_id": f"{identifier}:{sequence}",
                       "sequence": sequence, "kind": kind, "sent_ns": time.time_ns(),
                       "duration_ms": duration_ms, "padding": "x" * padding}
            sequence += 1
            before = time.perf_counter_ns()
            if mode == "sse":
                for channel in channels or [stream]:
                    publish(db, channel, payload)
            elif mode == "bus":
                for channel in channels or [stream]:
                    request.env["bus.bus"]._sendone(channel, "sse.experiment", payload)
            else:
                # Plain progress only. This cursor never reads business records.
                with Registry(db).cursor() as cr:
                    env = api.Environment(cr, SUPERUSER_ID, {})
                    for channel in channels or [stream]:
                        env["bus.bus"]._sendone(channel, "sse.experiment", payload)
                    cr.commit()
            costs.append((time.perf_counter_ns() - before) / 1e6)

        records = request.env["sse.experiment.probe"].sudo().browse(probes).exists()
        if probes and len(records) != 2:
            raise ValidationError("Missing probe pair")
        if not probes:
            for _ in range(count):
                emit(event_kind)
            return {"run_id": identifier, "cost_ms": costs, "pid": os.getpid()}
        if model_response:
            emit("model.response_received")
        emit("tool.started")
        records[0].value = 1
        records.flush_recordset(["value"])
        emit("tool.progress")
        time.sleep(duration_ms / 1000)
        records[1].value = 1
        records.flush_recordset(["value"])
        for _ in range(max(0, count - 3)):
            emit("tool.progress")
        emit("attempt.failed" if fail else "tool.returned")
        audit({"run_id": identifier, "outcome": "request_end", "at_ns": time.time_ns(),
               "started_ns": started, "cost_ms": costs, "mode": mode, "fail": fail,
               "db": db, "pid": os.getpid()})
        if fail:
            raise ValidationError("Intentional experiment rollback")
        return {"run_id": identifier, "started_ns": started, "cost_ms": costs,
                "end_ns": time.time_ns(), "pid": os.getpid()}

    @http.route("/sse_experiment/request_model", type="jsonrpc", auth="user")
    def request_model(self, params):
        self.admin()
        # Fixed local fake provider, which queues a callback and returns now.
        self.trial(stream=params["stream"], probes=[], mode=params["mode"],
                   duration_ms=0, count=1, run_id=params["run_id"],
                   event_kind="model.request_sent", event_offset=params["event_offset"])
        data = json.dumps({"params": params, "cookie": request.session.sid}).encode()
        upstream = urllib.request.Request("http://127.0.0.1:8081/model", data=data,
                                          headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(upstream, timeout=2) as response:
            return json.load(response)

    @http.route("/sse_experiment/health", type="jsonrpc", auth="user")
    def health(self):
        self.admin()
        return {"pid": os.getpid(), "registry_lru": Registry.registries.count,
                "db": request.env.cr.dbname}

    @http.route("/sse_experiment", type="http", auth="user")
    def page(self):
        self.admin()
        return request.make_response((Path(__file__).parent / "static" / "tracer.html").read_text(),
                                     headers=[("Content-Type", "text/html")])
