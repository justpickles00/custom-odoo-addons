"""Standalone, database-free hook checks. Run with the Odoo series Python.

PYTHONPATH=/path/to/odoo python pos_bus_probe/tests/check_hooks.py
"""
import os
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

import odoo
from odoo import sql_db
from odoo.http.dispatcher import JsonRPCDispatcher
from odoo.modules.module import initialize_sys_path
from odoo.tools import config

config["addons_path"] = str(Path(__file__).resolve().parents[2]) + "," + str(Path(next(iter(odoo.__path__))).resolve().parent / "addons")
initialize_sys_path()

from odoo.addons.bus import bus_dispatcher
from odoo.addons.bus.models.bus import BusBus
from odoo.addons.bus.websocket import Websocket
from odoo.addons.bus.websocket_protocol import ConnectionState
from odoo.addons.pos_bus_probe import hooks

original_api = getattr(BusBus.create, "_api", None)
native_forward = bus_dispatcher.ChannelTopic._forward_notifications
native_dumps = bus_dispatcher.orjson.dumps
committed_connections = []
sql_db.PsycoConnection.commit = lambda connection: committed_connections.append(connection)
JsonRPCDispatcher.dispatch = lambda self, endpoint, args: "native-rpc-result"
os.environ["POS_BUS_PROBE_DIR"] = "/unused-by-mocked-recorder"
hooks.post_load()
events = []
hooks.emit = lambda kind, **fields: events.append({"kind": kind, **fields})


class HookChecks(unittest.TestCase):
    def tearDown(self):
        hooks._batch.set(None)
        events.clear()

    def test_create_decorator_metadata(self):
        self.assertEqual(getattr(BusBus.create, "_api", None), original_api)

    def test_row_metadata_exact_native_payload_size(self):
        for payload in (None, "雪", {"a": [1, "☃"], "b": False}, ["x"] * 100):
            values = {"channel": '["posbus_test","token"]',
                      "message": json.dumps({"type": "a\"☃", "payload": payload}, separators=(",", ":"))}
            metadata = hooks._row_metadata(7, values)
            self.assertEqual(metadata["payload_bytes"], len(json.dumps(payload, separators=(",", ":")).encode()))
            self.assertEqual(metadata["channel"], ["posbus_test", "token"])

    def test_database_free_authentication(self):
        dispatcher = SimpleNamespace(request=SimpleNamespace(db=None, env=None))
        self.assertEqual(JsonRPCDispatcher.dispatch(dispatcher, None, {}), "native-rpc-result")

    def test_exact_low_level_commit_mapping(self):
        connection = object()
        cursor = SimpleNamespace(_cnx=connection, dbname="posbus_test", _posbus_rows=[12, 13],
                                 _posbus_operation_id="test-op")
        token = hooks._commit_cursor.set(cursor)
        try:
            sql_db.PsycoConnection.commit(connection)
        finally:
            hooks._commit_cursor.reset(token)
        self.assertIs(committed_connections[-1], connection)
        self.assertEqual(events[-1]["bus_ids"], [12, 13])
        self.assertEqual(events[-1]["operation_id"], "test-op")
        self.assertLessEqual(events[-1]["commit_start_ns"], events[-1]["commit_return_ns"])
        self.assertEqual(cursor._posbus_rows, [])

    def test_native_notification_bytes_and_enqueue(self):
        frames = []
        websocket = object.__new__(Websocket)
        websocket.state = ConnectionState.OPEN
        websocket._enqueue_control_command = lambda command, frame: frames.append(frame)
        notifications = [{"id": 123, "message": {"type": "probe", "payload": {"token": "abc"}}}]
        batch = {}
        hooks._batch.set(batch)
        bus_dispatcher.ChannelTopic._forward_notifications(notifications, (websocket,))
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, native_dumps(notifications))
        self.assertEqual(batch["websocket_send_calls"], 1)
        self.assertEqual(batch["serialized_bytes"], len(frames[0].payload))
        self.assertGreaterEqual(batch["serialize_ns"], 0)
        self.assertGreaterEqual(batch["enqueue_ns"], 0)

    def test_queue_claim_guard(self):
        dispatcher = bus_dispatcher.BusDispatcher()
        topic = bus_dispatcher.ChannelTopic(("posbus_test", "probe"), 0)
        dispatcher._queue_topic(topic)
        self.assertIn(topic, dispatcher._claim("posbus_test"))
        self.assertGreaterEqual(hooks._batch.get()["queue_wait_min_ns"], 0)
        other = bus_dispatcher.ChannelTopic(("ordinary_db", "probe"), 0)
        dispatcher._queue_topic(other)
        dispatcher._claim("ordinary_db")
        self.assertIsNone(hooks._batch.get())
        self.assertFalse(hasattr(other, "_posbus_queued_ns"))

    def test_close_preserves_native_enqueue(self):
        frames = []
        websocket = object.__new__(Websocket)
        websocket.state = ConnectionState.OPEN
        websocket._db = "posbus_test"
        websocket._enqueue_control_command = lambda command, frame: frames.append(frame)
        websocket.close(1013, "OUTGOING_QUEUE_SATURATED")
        self.assertEqual(websocket.state, ConnectionState.CLOSING)
        self.assertEqual(frames, [{"code": 1013, "reason": "OUTGOING_QUEUE_SATURATED"}])
        self.assertEqual(events[-1]["reason"], "OUTGOING_QUEUE_SATURATED")


if __name__ == "__main__":
    unittest.main()
