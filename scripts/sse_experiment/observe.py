#!/usr/bin/env python3
"""Sample the tested Odoo process group without blocking the load generator."""
import argparse
import json
from pathlib import Path
import time

from load import resources


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=930)
    parser.add_argument("--output", type=Path, default=Path("/home/coder/playground/sse-utility-experiment/results/soak-resources.json"))
    args = parser.parse_args()
    records = []
    started = time.perf_counter()
    while time.perf_counter() - started < args.seconds:
        records.append({"at_ns":time.time_ns(),"elapsed_s":time.perf_counter()-started,
                        "processes":resources()})
        args.output.write_text(json.dumps(records,indent=2))
        print(f"resource sample at {records[-1]['elapsed_s']:.0f}s",flush=True)
        time.sleep(30)
