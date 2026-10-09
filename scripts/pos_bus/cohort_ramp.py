#!/usr/bin/env python3
"""Run four disjoint five-tenant producer/receiver cohorts behind one warm gate."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

parser=argparse.ArgumentParser()
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--rate',type=float,default=40000,help='Total offered rows/sec across all four processes')
parser.add_argument('--duration',type=float,default=60)
args=parser.parse_args()
args.output.mkdir(parents=True,exist_ok=False)
root=Path('/home/coder/playground/pos-bus-investigation')
gate=args.output.resolve()/'GLOBAL_GO'
children=[]
try:
    for i in range(4):
        output=args.output.resolve()/f'cohort-{i}'
        command=[sys.executable,str(Path(__file__).with_name('bus_ramp.py')),
                 '--fixture',str(root/'fixture.json'),'--config',str(root/'odoo.conf'),
                 '--tenants','5','--db-start',str(i*5),'--tills','4','--producer-count','1',
                 '--batch-period','.5','--rates',str(args.rate/4),'--duration',str(args.duration),
                 '--compact-receipts','--startup-concurrency','1','--startup-timeout','300',
                 '--drain-timeout','90','--global-go',str(gate),'--output',str(output)]
        log=(args.output/f'cohort-{i}.log').open('w')
        children.append((subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT),output,log))
    deadline=time.monotonic()+300
    while not all((output/'RECEIVERS_READY').exists() for _,output,_ in children):
        if any(process.poll() is not None for process,_,_ in children):
            raise RuntimeError('A cohort exited before readiness; inspect its log')
        if time.monotonic()>deadline:
            raise TimeoutError('Cohort readiness timed out')
        time.sleep(.5)
    gate.touch()
    print(json.dumps({'event':'all_cohorts_warm','wall_ns':time.time_ns(),'offered_global_rate':args.rate}),flush=True)
    while any(process.poll() is None for process,_,_ in children):
        if time.monotonic()>deadline+args.duration+240:
            raise TimeoutError('Cohort run exceeded bounded deadline')
        time.sleep(1)
    result={'offered_global_rate':args.rate,'duration':args.duration,'exit_codes':[process.returncode for process,_,_ in children]}
    (args.output/'cohorts.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)
finally:
    for process,_,log in children:
        if process.poll() is None:
            process.terminate()
    for process,_,log in children:
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        log.close()
