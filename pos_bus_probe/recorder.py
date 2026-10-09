"""Buffered process-local recording; no synchronous disk writes in measured paths."""
import atexit
import collections
import json
import logging
import os
import threading
import time

_logger = logging.getLogger(__name__)
_recorder = None
_init_lock = threading.Lock()


def enabled(dbname):
    return bool(os.getenv("POS_BUS_PROBE_DIR")) and isinstance(dbname, str) and dbname.startswith("posbus_")


class Recorder:
    def __init__(self, directory, capacity=100_000):
        self.pid = os.getpid()
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, f"probe-{self.pid}.jsonl")
        self.pending = collections.deque()
        self.lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.capacity = capacity
        self.dropped = 0
        self.emit_ns = 0
        self.emit_count = 0
        self.thread = threading.Thread(target=self._loop, name="pos-bus-probe-writer", daemon=True)
        self.thread.start()
        atexit.register(self.flush)

    def emit(self, kind, **fields):
        start = time.monotonic_ns()
        record = {"kind": kind, "pid": self.pid, "monotonic_ns": start, **fields}
        with self.lock:
            if len(self.pending) < self.capacity:
                self.pending.append(record)
            else:
                self.dropped += 1
            self.emit_count += 1
            self.emit_ns += time.monotonic_ns() - start

    def _loop(self):
        while True:
            time.sleep(0.5)
            self.flush()

    def flush(self):
        if os.getpid() != self.pid:
            # Parent atexit callbacks survive fork; never replay their copied
            # pending records from a worker into the parent's log.
            return
        with self.write_lock:
            with self.lock:
                records, self.pending = self.pending, collections.deque()
                stats = {"kind": "recorder_stats", "pid": self.pid,
                         "monotonic_ns": time.monotonic_ns(), "dropped_total": self.dropped,
                         "emit_count_total": self.emit_count, "emit_ns_total": self.emit_ns,
                         "stages_enabled": os.getenv("POS_BUS_PROBE_STAGES", "1") != "0"}
            if not records:
                return
            start = time.monotonic_ns()
            try:
                encoded = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in records)
                stats["encode_ns"] = time.monotonic_ns() - start
                stats["record_count"] = len(records)
                with open(self.path, "a", encoding="utf-8") as stream:
                    stream.write(encoded)
                    stats["write_ns"] = time.monotonic_ns() - start - stats["encode_ns"]
                    stream.write(json.dumps(stats, separators=(",", ":")) + "\n")
            except Exception:
                _logger.exception("POS Bus probe log flush failed")


def recorder():
    global _recorder
    # Odoo may fork after importing/loading the server-wide module.
    if _recorder is None or _recorder.pid != os.getpid():
        with _init_lock:
            if _recorder is None or _recorder.pid != os.getpid():
                _recorder = Recorder(os.environ["POS_BUS_PROBE_DIR"])
    return _recorder


def emit(kind, **fields):
    try:
        recorder().emit(kind, **fields)
    except Exception:
        # Measurement failures must not turn a successful business operation
        # into an application failure (e.g. if the output directory disappears).
        _logger.exception("POS Bus probe record failed")


def _after_fork():
    global _recorder, _init_lock
    _recorder = None
    _init_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
