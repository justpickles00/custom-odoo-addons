#!/usr/bin/env python3
"""Join ramp windows to external CPU samples and optional dispatcher spans."""
import argparse
from collections import defaultdict
import json
from pathlib import Path

from analyze import stats

def rows(path):
    for line in path.open():
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue

def role(command):
    if '/bin/bash' in command:
        return None
    if 'odoo-bin gevent' in command:
        return 'gevent'
    if 'odoo-bin shell' in command:
        return 'producer'
    if 'bus_ramp.py' in command or 'restaurant_load.py' in command:
        return 'client'
    if 'postgres:' in command:
        return 'postgres'
    if 'odoo-bin' in command:
        return 'odoo_other'

def analyze(directory, telemetry, probe):
    summary = json.loads((directory/'summary.json').read_text())
    windows = summary.get('cases', [])
    if not windows:
        client_rows = list(rows(directory/'clients.jsonl'))
        start = next(r for r in client_rows if r['event']=='measurement_start')
        end = next(r for r in client_rows if r['event']=='measurement_end')
        windows = [{'case':'restaurant', 'start_ns': start['scheduled_start_ns'], 'end_ns': end['scheduled_end_ns']}]
    result = []
    samples = list(rows(telemetry))
    batches = []
    for path in probe.glob('probe-*.jsonl'):
        for r in rows(path):
            if r['kind']=='dispatch_batch':
                batches.append(r)
    for window in windows:
        start, end = window['start_ns'], window['end_ns']
        cpu = defaultdict(list)
        selected = [r for r in samples if start<=r['mono_ns']<=end]
        for before, after in zip(selected, selected[1:]):
            dt = (after['mono_ns']-before['mono_ns'])/1e9
            old = {p['pid']:p for p in before['processes']}
            totals = defaultdict(float)
            for proc in after['processes']:
                name = role(proc['cmd'])
                if name and proc['pid'] in old:
                    totals[name] += (proc['ticks']-old[proc['pid']]['ticks'])/after['ticks_per_second']/dt*100
            for name, value in totals.items():
                cpu[name].append(value)
        spans = [b for b in batches if start<=b['start_ns']<=end]
        stage_names = ('queue_wait_max_ns','cursor_acquire_ns','session_validation_ns','fetch_ns','dispatch_ns','duration_ns')
        result.append({'case':window['case'], 'cpu_percent_one_core': {k:stats(v) for k,v in cpu.items()},
                       'pg_activity_rows_peak_including_background':max((sum(x[3] for x in r['pg']) for r in selected), default=0),
                       'stage_wall_ms':{name:stats([b.get(name,0) for b in spans],1e6) for name in stage_names},
                       'matched_dispatch_batches':len(spans)})
    output = {'cases':result,'limits':'CPU samples omit short-lived processes between samples; stages are overlapping elapsed wall time, not CPU. All processes share one VM. PostgreSQL activity count includes background workers.'}
    (directory/'resource-analysis.json').write_text(json.dumps(output,indent=2)+'\n')
    return output

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('directory',type=Path)
    parser.add_argument('--telemetry',type=Path,required=True)
    parser.add_argument('--probe',type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(analyze(args.directory,args.telemetry,args.probe),indent=2))
