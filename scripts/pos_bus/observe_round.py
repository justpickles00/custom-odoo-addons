#!/usr/bin/env python3
"""Read-only host/PostgreSQL/process sampling for isolated ramp experiments."""
import argparse
import json
import os
from pathlib import Path
import time

import psycopg2

parser = argparse.ArgumentParser()
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--duration', type=float, default=3600)
args = parser.parse_args()
connection = psycopg2.connect(dbname='postgres', user='coder', host='/var/run/postgresql', application_name='posbus-observer')
connection.autocommit = True
end = time.monotonic() + args.duration
with args.output.open('w', buffering=1) as stream:
    while time.monotonic() < end:
        processes = []
        for path in Path('/proc').iterdir():
            if not path.name.isdigit():
                continue
            try:
                cmd = (path/'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
                if not any(term in cmd for term in ('odoo-bin', 'bus_ramp.py', 'restaurant_load.py', 'postgres:')):
                    continue
                stat = (path/'stat').read_text().split()
                processes.append({'pid': int(path.name), 'ppid': int(stat[3]), 'ticks': int(stat[13])+int(stat[14]), 'rss': int(stat[23])*os.sysconf('SC_PAGE_SIZE'), 'cmd': cmd})
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                pass
        with connection.cursor() as cr:
            cr.execute('SELECT datname,state,wait_event_type,count(*) FROM pg_stat_activity GROUP BY 1,2,3')
            pg = cr.fetchall()
        stream.write(json.dumps({'mono_ns': time.monotonic_ns(), 'wall_ns': time.time_ns(), 'ticks_per_second': os.sysconf('SC_CLK_TCK'), 'processes': processes, 'pg': pg, 'load': os.getloadavg(), 'memory': (Path('/proc/meminfo').read_text())})+'\n')
        time.sleep(1)
connection.close()
