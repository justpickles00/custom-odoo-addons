import os

from werkzeug.exceptions import NotFound

from odoo.http import Controller, request, route

from .recorder import enabled, recorder


class ProbeController(Controller):
    def _guard(self):
        if not enabled(request.env.cr.dbname):
            raise NotFound()

    @route("/pos_bus_probe/publish", type="jsonrpc", auth="user", methods=["POST"])
    def publish(self, channel, notification_type, payload, operation_id):
        self._guard()
        if not all(isinstance(value, str) and value for value in (channel, notification_type, operation_id)):
            raise ValueError("channel, notification_type and operation_id must be nonempty strings")
        request.env.cr._posbus_operation_id = operation_id
        request.env["bus.bus"]._sendone(channel, notification_type, payload)
        return {"operation_id": operation_id, "queued": True}

    @route("/pos_bus_probe/flush", type="jsonrpc", auth="user", methods=["POST"])
    def flush(self):
        self._guard()
        recorder().flush()
        return {"pid": os.getpid(), "path": recorder().path}
