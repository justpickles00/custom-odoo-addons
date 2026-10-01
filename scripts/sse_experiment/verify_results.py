#!/usr/bin/env python3
"""Check the report's key assertions against preserved experiment artifacts."""
import argparse
import hashlib
import json
from pathlib import Path


def verify(directory):
    def read(name):
        return json.loads((directory / name).read_text())

    latency = read("latency.json")
    assert len(latency) == 246
    assert sum(r["fail"] for r in latency) == 123
    assert all(r["during_values"] == [0, 0] for r in latency)
    assert all(r["after_values"] == ([0, 0] if r["fail"] else [1, 1]) for r in latency)
    assert all(r["http_error"] == r["fail"] for r in latency)
    for row in latency:
        expected = 0 if row["fail"] and row["mode"] == "bus" else 4
        assert row["events_received"] == expected
        assert [s["sequence"] for s in row["samples"]] == list(range(expected))
    load = read("load-sized.json")
    assert len(load) == 36
    assert sum(r["expected_deliveries"] for r in load) == 2182500
    assert all(r["received_deliveries"] == r["expected_deliveries"] and not r["errors"] for r in load)
    for row in read("loop.json"):
        assert [v for _, v in row["probes"]] == [1, 1, 1, 1, 0, 0]
        assert len(row["events"]) == (13 if row["mode"] == "bus" else 18)
    renewal = read("renewal.json")
    assert len(renewal["offered"]) == 920 and all(r["accepted"] for r in renewal["offered"])
    assert len(renewal["received"]) == 917
    assert sum(r["state"] == "live" for r in renewal["states"]) == 4
    sse = read("soak-sse.json")[0]
    bus = read("soak-bus.json")[0]
    assert sse["offered_events"] == 180000 and sse["received_deliveries"] == 179471
    assert len(sse["publish_errors"]) == 538 and sse["renewals"] == 24576
    assert bus["received_deliveries"] == bus["expected_deliveries"] == 1440000
    assert not bus["publish_errors"]
    fault = read("fault.json")
    assert not fault[0]["request_failed"] and fault[0]["values"] == [1, 1]
    assert fault[1]["request_failed"] and fault[1]["values"] == [0, 0]
    for name, accepted in [("restored-gate-tenant.json", 128), ("restored-gate-global.json", 1024)]:
        result = read(name)[0]
        assert result["ready"] == accepted and result["connections"] == accepted + 1
        assert result["connection_errors"] == ["connect status 503"]
        assert result["received_deliveries"] == result["expected_deliveries"]
    metadata = read("metadata.json")
    for name, digest in metadata["production_source_sha256"].items():
        assert hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest
    assert read("provision.json")["count"] == 1000
    print("Verified rollback, browser/loop receipt, load counts, renewal/fault findings, restored gates, and unchanged production source.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path, nargs="?", default=Path("docs/sse-results"))
    verify(parser.parse_args().directory)
