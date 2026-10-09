#!/usr/bin/env python3
"""Observe only the experiment's Odoo process tree and host resource headroom."""
import argparse
import json
import os
import time
from pathlib import Path


def sample():
    processes = []
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():
            continue
        try:
            cmd = (p / 'cmdline').read_bytes().replace(b'\0', b' ').decode()
            if 'odoo-bin' not in cmd or 'pos-bus-investigation/odoo.conf' not in cmd:
                continue
            stat = (p / 'stat').read_text().split()
            processes.append({'pid': int(p.name), 'ppid': int(stat[3]),
                              'cpu_ticks': int(stat[13])+int(stat[14]),
                              'rss_bytes': int(stat[23])*os.sysconf('SC_PAGE_SIZE'),
                              'fds': len(list((p / 'fd').iterdir())),
                              'command': cmd})
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    memory = {line.split(':')[0]: line.split(':')[1].strip() for line in Path('/proc/meminfo').read_text().splitlines()}
    return {'mono_ns': time.monotonic_ns(), 'wall_ns': time.time_ns(), 'processes': processes,
            'loadavg': os.getloadavg(), 'mem_available': memory['MemAvailable'],
            'clock_ticks': os.sysconf('SC_CLK_TCK')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--duration', type=float, default=3600)
    args = parser.parse_args()
    end = time.monotonic() + args.duration
    with args.output.open('a', buffering=1) as stream:
        while time.monotonic() < end:
            stream.write(json.dumps(sample())+'\n')
            time.sleep(2)
