#!/usr/bin/env python3
"""Stop only this isolated Odoo parent before the host runs out of memory."""
import argparse
import json
import os
from pathlib import Path
import signal
import time

parser = argparse.ArgumentParser()
parser.add_argument('--pid', type=int, required=True)
parser.add_argument('--minimum-mib', type=int, default=1200)
args = parser.parse_args()
proc = Path('/proc') / str(args.pid)
marker = b'/home/coder/playground/pos-bus-investigation/odoo.conf'
assert marker in (proc / 'cmdline').read_bytes(), 'not the experiment process'
while proc.exists():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    available_kib = int(values['MemAvailable'].split()[0])
    if available_kib < args.minimum_mib * 1024:
        assert marker in (proc / 'cmdline').read_bytes()
        print(json.dumps({'action':'stop_experiment', 'pid':args.pid, 'available_kib':available_kib, 'wall_ns':time.time_ns()}), flush=True)
        os.kill(args.pid, signal.SIGTERM)
        break
    time.sleep(.5)
