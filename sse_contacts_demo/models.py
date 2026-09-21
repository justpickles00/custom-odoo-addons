from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import logging
import time

from odoo import api, fields, models, SUPERUSER_ID
from odoo.exceptions import AccessError
from odoo.orm.registry import Registry

from odoo.addons.sse_server.api import publish, runtime_dir

_logger = logging.getLogger(__name__)
STREAM = "contacts.activity"


@contextmanager
def publication(database):
    path = runtime_dir() / (hashlib.sha256(database.encode()).hexdigest() + ".lock")
    with path.open("a") as lock:
        deadline = time.monotonic() + 2
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Contacts publisher busy") from None
                time.sleep(0.01)
        try:
            # Start the count's database snapshot only after acquiring the lock.
            with Registry(database).cursor() as cr:
                cr.execute("SET LOCAL statement_timeout = '2s'")
                yield api.Environment(cr, SUPERUSER_ID, {"active_test": False})
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def snapshot(database):
    with publication(database) as env:
        payload = {"operation": "snapshot", "count": env["res.partner"].search_count([])}
        return {"payload": payload, **publish(database, STREAM, payload)}


def deliver(database, ids):
    try:
        with publication(database) as env:
            events = env["sse.contacts.event"].browse(ids).exists()
            payloads = [event.payload for event in events]
            count = None
            if any(p["operation"] in ("create", "unlink") for p in payloads):
                count = env["res.partner"].search_count([])
            for payload in payloads:
                if payload["operation"] in ("create", "unlink"):
                    payload = {**payload, "count": count}
                publish(database, STREAM, payload)
            events.unlink()
            env.cr.commit()
    except Exception:
        # Business changes have already committed; a UI outage must not fail them.
        _logger.warning("Contacts live publication failed; reconnect to resync", exc_info=True)


class ContactEvent(models.TransientModel):
    _name = "sse.contacts.event"
    _description = "Committed contacts event staging"
    _transient_max_hours = 1

    payload = fields.Json(required=True)


class StreamAccess(models.AbstractModel):
    _inherit = "sse.stream"

    def _authorize(self, stream):
        if stream != STREAM:
            return super()._authorize(stream)
        if not self.env.user.has_group("base.group_system"):
            raise AccessError("Only administrators may view contacts activity.")


class ResPartner(models.Model):
    _inherit = "res.partner"

    def _stage_activity(self, operation, record_ids=None, field_names=None):
        if not self.env.registry.ready:
            return
        ids = self.ids if record_ids is None else record_ids
        if not ids:
            return
        payload = {
            "operation": operation, "record_ids": ids,
            "actor_user_id": self.env.uid, "superuser": self.env.su,
            "fields": field_names or [],
            "time": datetime.now(timezone.utc).isoformat(),
        }
        event = self.env["sse.contacts.event"].sudo().create({"payload": payload})
        key = "sse.contacts.events"
        callbacks = self.env.cr.postcommit
        if key not in callbacks.data:
            pending = callbacks.data[key] = []
            database = self.env.cr.dbname
            callbacks.add(lambda: deliver(database, pending))
        callbacks.data[key].append(event.id)

    @api.model_create_multi
    def create(self, values_list):
        records = super().create(values_list)
        records._stage_activity("create")
        return records

    def write(self, values):
        result = super().write(values)
        self._stage_activity("write", field_names=list(values))
        return result

    def unlink(self):
        ids = self.ids
        result = super().unlink()
        self._stage_activity("unlink", record_ids=ids)
        return result
