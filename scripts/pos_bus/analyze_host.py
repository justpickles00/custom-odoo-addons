#!/usr/bin/env python3
"""Read-only host-counter window analysis; emit JSON to stdout."""
import argparse
import json
from pathlib import Path
from analyze import rows, stats

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('directory',type=Path)
parser.add_argument('--counters',type=Path,required=True)
args=parser.parse_args()
clients=list(rows(args.directory/'clients.jsonl'))
start=next(r['scheduled_start_ns'] for r in clients if r['event']=='measurement_start')
end=next(r['scheduled_end_ns'] for r in clients if r['event']=='measurement_end')
samples=[r for r in rows(args.counters) if start<=r['mono_ns']<=end]
if len(samples)<2:raise ValueError('At least two samples inside measurement window required')
elapsed=(samples[-1]['mono_ns']-samples[0]['mono_ns'])/1e9
names=['user','nice','system','idle','iowait','irq','softirq','steal']
cpus={name:[] for name in names}
for a,b in zip(samples,samples[1:]):
    delta=[y-x for x,y in zip(a['cpu_ticks'][:8],b['cpu_ticks'][:8])]
    if any(x<0 for x in delta) or sum(delta)<=0:raise ValueError('Invalid CPU counter delta')
    for name,x in zip(names,delta):cpus[name].append(x/sum(delta)*100)
first={r[0]:r for r in samples[0]['databases']}
last={r[0]:r for r in samples[-1]['databases']}
active={r['db'] for r in clients if r['event']=='device_started'}
databases={}
for db in sorted(active|{'postgres'}):
    a,b=first[db],last[db]
    if a[4]!=b[4] or b[1]<a[1]:raise ValueError('Database statistics reset within window: '+db)
    databases[db]={'new_sessions':b[1]-a[1],'new_sessions_per_s':(b[1]-a[1])/elapsed}
print(json.dumps({'sample_count':len(samples),'sample_start_ns':samples[0]['mono_ns'],
    'sample_end_ns':samples[-1]['mono_ns'],'sample_span_s':elapsed,
    'cpu_percent_whole_host':{k:stats(v) for k,v in cpus.items()},'database_sessions':databases,
    'active_tenant_new_sessions_per_s':sum(databases[k]['new_sessions_per_s'] for k in active),
    'limits':'Whole shared-host CPU, not Odoo-only CPU. Database cumulative statistics have publication lag; postgres includes other system-DB clients. These samples began late in round3 and cannot be extrapolated to earlier cases.'},indent=2))
