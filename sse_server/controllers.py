from odoo import http, models
from odoo.exceptions import AccessError
from odoo.http import request

from .api import ticket


class StreamAccess(models.AbstractModel):
    _name = "sse.stream"
    _description = "SSE stream authorization"

    def _authorize(self, stream):
        raise AccessError("This stream is not available.")


class SSEController(http.Controller):
    @http.route("/sse/connect", type="jsonrpc", auth="user")
    def connect(self, stream):
        request.env["sse.stream"]._authorize(stream)
        value, scope = ticket(
            request.env.cr.dbname, request.env.uid, request.session.sid, stream,
        )
        return {"url": f"/sse/stream?ticket={value}", "scope": scope,
                "database": request.env.cr.dbname}
