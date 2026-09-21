from odoo import http
from odoo.http import request

from .models import STREAM, snapshot


class ContactsController(http.Controller):
    @http.route("/sse_contacts_demo/snapshot", type="jsonrpc", auth="user")
    def contacts_snapshot(self):
        request.env["sse.stream"]._authorize(STREAM)
        return snapshot(request.env.cr.dbname)
