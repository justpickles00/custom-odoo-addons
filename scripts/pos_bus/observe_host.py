#!/usr/bin/env python3
"""Read-only whole-host CPU/pressure and cumulative PostgreSQL connection counters.

Counters cover this shared host/database lifetime. Compare timestamped deltas,
check stats_reset, and allow PostgreSQL statistics publication lag. These are
not exact per-request attribution or proof that all CPU belongs to Odoo.
"""
import argparse
import json
from pathlib import Path
import time

import psycopg2

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--duration', type=float, default=3600)
args = parser.parse_args()
connection = psycopg2.connect(dbname='postgres', user='coder', host='/var/run/postgresql', application_name='posbus-host-observer')
connection.autocommit = True
end = time.monotonic() + args.duration
with args.output.open('x', buffering=1) as output:
    while time.monotonic() < end:
        with connection.cursor() as cursor:
            cursor.execute("SELECT datname,sessions,session_time,active_time,stats_reset FROM pg_stat_database WHERE datname LIKE 'posbus_%%' OR datname='postgres'")
            databases = cursor.fetchall()
        output.write(json.dumps({'mono_ns': time.monotonic_ns(), 'wall_ns': time.time_ns(),
            'cpu_ticks': list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:])),
            'pressure': {kind: Path('/proc/pressure', kind).read_text() for kind in ('cpu','io','memory')},
            'databases': databases}, default=str) + '\n')
        time.sleep(1)
connection.close()
