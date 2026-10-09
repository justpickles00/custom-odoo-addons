"""Opt-in wrappers for Odoo master 5bcf314eb0896bdf7e218341a134901004ba3d4d.

Every original callable is invoked exactly once with unchanged arguments. The
low-level commit clock is sampled before constructing or recording any event.
"""
import contextvars
import functools
import itertools
import json
import os
import time
from contextlib import contextmanager

from .recorder import emit, enabled

_installed = False
_commit_cursor = contextvars.ContextVar("posbus_commit_cursor", default=None)
_batch = contextvars.ContextVar("posbus_dispatch_batch", default=None)
_forward = contextvars.ContextVar("posbus_forward", default=False)
_batch_ids = itertools.count(1)


def _add(stage, elapsed):
    batch = _batch.get()
    if batch is not None:
        batch[stage] = batch.get(stage, 0) + elapsed


def _row_metadata(row_id, value):
    message = json.loads(value["message"])
    # Native json_dump uses compact separators and ASCII escaping. Subtract
    # the small envelope without reserializing large payloads.
    envelope = json.dumps({"type": message["type"], "payload": None}, separators=(",", ":"))
    payload_bytes = len(value["message"].encode()) - len(envelope.encode()) + len("null")
    return {"id": row_id, "channel": json.loads(value["channel"]),
            "type": message["type"], "payload_bytes": payload_bytes}


def post_load():
    global _installed
    if _installed or not os.getenv("POS_BUS_PROBE_DIR"):
        return
    _installed = True
    from odoo import sql_db
    from odoo.http.dispatcher import JsonRPCDispatcher
    from odoo.addons.bus import bus_dispatcher as dispatcher
    from odoo.addons.bus.models.bus import BusBus

    original_create = BusBus.create

    @functools.wraps(original_create)
    def create(self, values):
        records = original_create(self, values)
        cr = self.env.cr
        if enabled(cr.dbname):
            # Odoo's own precommit create_bus callback invokes this method. IDs
            # therefore belong to this cursor's real transaction, including
            # payloads deferred or skipped by native _prepare_payload logic.
            pending = cr.__dict__.get("_posbus_rows")
            if pending is None:
                cr._posbus_rows = pending = []
            pending.extend(records.ids)
            metadata = cr.__dict__.setdefault("_posbus_row_metadata", [])
            for row_id, value in zip(records.ids, [values] if isinstance(values, dict) else values):
                try:
                    metadata.append(_row_metadata(row_id, value))
                except Exception as exc:
                    emit("metadata_error", db=cr.dbname, bus_id=row_id, error=type(exc).__name__)
        return records

    BusBus.create = create
    original_cursor_commit = sql_db.Cursor.commit

    @functools.wraps(original_cursor_commit)
    def cursor_commit(self):
        if not enabled(self.dbname):
            return original_cursor_commit(self)
        token = _commit_cursor.set(self)
        start = time.monotonic_ns()
        operation_id = self.__dict__.get("_posbus_operation_id")
        success = False
        try:
            result = original_cursor_commit(self)
            success = True
            return result
        finally:
            end = time.monotonic_ns()
            _commit_cursor.reset(token)
            emit("cursor_commit", db=self.dbname, operation_id=operation_id,
                 start_ns=start, end_ns=end, duration_ns=end - start, success=success)

    sql_db.Cursor.commit = cursor_commit
    original_low_commit = sql_db.PsycoConnection.commit

    @functools.wraps(original_low_commit)
    def low_commit(self):
        cr = _commit_cursor.get()
        if cr is None or cr._cnx is not self:
            return original_low_commit(self)
        start = time.monotonic_ns()
        result = original_low_commit(self)
        committed = time.monotonic_ns()
        rows = cr.__dict__.get("_posbus_rows", [])
        metadata = cr.__dict__.get("_posbus_row_metadata", [])
        cr._posbus_rows = []
        cr._posbus_row_metadata = []
        emit("commit_return", db=cr.dbname, operation_id=cr.__dict__.get("_posbus_operation_id"),
             bus_ids=rows, bus_rows=metadata, commit_start_ns=start, commit_return_ns=committed,
             commit_duration_ns=committed - start)
        return result

    sql_db.PsycoConnection.commit = low_commit
    original_rollback = sql_db.Cursor.rollback

    @functools.wraps(original_rollback)
    def rollback(self):
        if enabled(self.dbname):
            self._posbus_rows = []
            self._posbus_row_metadata = []
        return original_rollback(self)

    sql_db.Cursor.rollback = rollback
    original_savepoint_init = sql_db.Savepoint.__init__
    original_savepoint_rollback = sql_db.Savepoint.rollback

    @functools.wraps(original_savepoint_init)
    def savepoint_init(self, cr):
        original_savepoint_init(self, cr)
        if enabled(getattr(cr, "dbname", None)):
            self._posbus_row_count = len(cr.__dict__.get("_posbus_rows", ()))

    @functools.wraps(original_savepoint_rollback)
    def savepoint_rollback(self):
        result = original_savepoint_rollback(self)
        if hasattr(self, "_posbus_row_count"):
            pending = self._cr.__dict__.get("_posbus_rows", [])
            del pending[self._posbus_row_count:]
            metadata = self._cr.__dict__.get("_posbus_row_metadata", [])
            retained_ids = set(pending)
            metadata[:] = [row for row in metadata if row["id"] in retained_ids]
        return result

    sql_db.Savepoint.__init__ = savepoint_init
    sql_db.Savepoint.rollback = savepoint_rollback
    original_rpc = JsonRPCDispatcher.dispatch

    @functools.wraps(original_rpc)
    def rpc(self, endpoint, args):
        req = self.request
        db = getattr(req, "db", None)
        if not enabled(db) or req.env is None:
            return original_rpc(self, endpoint, args)
        op = req.httprequest.headers.get("X-Pos-Bus-Probe-Op")
        if op:
            req.env.cr._posbus_operation_id = op
        start = time.monotonic_ns()
        try:
            return original_rpc(self, endpoint, args)
        finally:
            end = time.monotonic_ns()
            emit("rpc_dispatch", db=db, operation_id=req.env.cr.__dict__.get("_posbus_operation_id"),
                 path=req.httprequest.path, start_ns=start, end_ns=end, duration_ns=end - start)

    JsonRPCDispatcher.dispatch = rpc
    if os.getenv("POS_BUS_PROBE_STAGES", "1") == "0":
        return
    cls = dispatcher.BusDispatcher
    original_queue = cls._queue_topic

    @functools.wraps(original_queue)
    def queue_topic(self, topic):
        if enabled(topic._dbname):
            topic._posbus_queued_ns = time.monotonic_ns()
        return original_queue(self, topic)

    cls._queue_topic = queue_topic
    original_claim = cls._claim

    @functools.wraps(original_claim)
    def claim(self, dbname):
        start = time.monotonic_ns()
        topics = original_claim(self, dbname)
        if topics and enabled(dbname):
            end = time.monotonic_ns()
            queued = [getattr(topic, "_posbus_queued_ns", start) for topic in topics]
            _batch.set({"db": dbname, "batch_id": next(_batch_ids), "start_ns": start,
                        "claim_ns": end - start, "topics": len(topics),
                        "queue_wait_max_ns": end - min(queued),
                        "queue_wait_min_ns": end - max(queued)})
        else:
            _batch.set(None)
        return topics

    cls._claim = claim
    original_acquire = dispatcher.acquire_cursor

    @contextmanager
    def acquire(db):
        start = time.monotonic_ns()
        with original_acquire(db) as cr:
            _add("cursor_acquire_ns", time.monotonic_ns() - start)
            yield cr

    dispatcher.acquire_cursor = acquire

    def timed_method(name, stage):
        original = getattr(cls, name)

        @functools.wraps(original)
        def timed(self, *args, **kwargs):
            if _batch.get() is None:
                return original(self, *args, **kwargs)
            start = time.monotonic_ns()
            end = None
            try:
                result = original(self, *args, **kwargs)
                end = time.monotonic_ns()
                if name == "_fetch":
                    batch = _batch.get()
                    batch["bus_ids"] = [n["id"] for values in result.values() for n in values]
                    batch["channels_with_rows"] = len(result)
                return result
            finally:
                _add(stage, (end if end is not None else time.monotonic_ns()) - start)

        setattr(cls, name, timed)

    timed_method("_kick_invalid_sessions", "session_validation_ns")
    timed_method("_fetch", "fetch_ns")
    timed_method("_dispatch", "dispatch_ns")
    original_dumps = dispatcher.orjson.dumps

    @functools.wraps(original_dumps)
    def dumps(*args, **kwargs):
        if not _forward.get():
            return original_dumps(*args, **kwargs)
        start = time.monotonic_ns()
        result = original_dumps(*args, **kwargs)
        _add("serialize_ns", time.monotonic_ns() - start)
        _add("serialized_bytes", len(result))
        return result

    dispatcher.orjson.dumps = dumps
    original_forward = dispatcher.ChannelTopic._forward_notifications

    @functools.wraps(original_forward)
    def forward(notifications, websockets):
        if _batch.get() is None or not notifications:
            return original_forward(notifications, websockets)
        token = _forward.set(True)
        start = time.monotonic_ns()
        try:
            return original_forward(notifications, websockets)
        finally:
            _add("forward_ns", time.monotonic_ns() - start)
            _add("forward_calls", 1)
            _forward.reset(token)

    dispatcher.ChannelTopic._forward_notifications = staticmethod(forward)
    from odoo.addons.bus.websocket import Websocket
    original_send = Websocket.send

    @functools.wraps(original_send)
    def send(self, *args, **kwargs):
        if not _forward.get():
            return original_send(self, *args, **kwargs)
        start = time.monotonic_ns()
        try:
            return original_send(self, *args, **kwargs)
        finally:
            _add("enqueue_ns", time.monotonic_ns() - start)
            _add("websocket_send_calls", 1)

    Websocket.send = send
    original_release = cls._release_claims

    @functools.wraps(original_release)
    def release(self, dbname, topics, *, rollback=False):
        batch = _batch.get()
        try:
            return original_release(self, dbname, topics, rollback=rollback)
        finally:
            if batch is not None:
                end = time.monotonic_ns()
                batch.update(end_ns=end, duration_ns=end - batch["start_ns"], rollback=rollback)
                _batch.set(None)
                emit("dispatch_batch", **batch)

    cls._release_claims = release
