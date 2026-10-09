#!/usr/bin/env python3
"""Sequential controlled windows. Requires the isolated lab server already running."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

from analyze import analyze

ROOT = Path('/home/coder/playground/pos-bus-investigation')
SCRIPTS = Path(__file__).resolve().parent
CASES = {
    'fleet-50-paced': ['--tenants','50','--duration','60'],
    'fleet-100-paced': ['--tenants','100','--duration','60'],
    'fleet-50-bounded': ['--tenants','50','--duration','60'],
    'fleet-100-coalesced': ['--tenants','100','--duration','60','--coalesce-equal-sync'],
    'fleet-20-passive': ['--tenants','20','--duration','60','--mode','passive'],
    'fleet-20-coalesced': ['--tenants','20','--duration','60','--coalesce-equal-sync'],
    'fleet-20-native-repeat': ['--tenants','20','--duration','60'],
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cases', nargs='+', choices=CASES)
    args = parser.parse_args()
    for name in args.cases:
        output = ROOT / 'results' / name
        assert not output.exists(), f'Will not overwrite {output}'
        command = [sys.executable,str(SCRIPTS/'run.py'),'--fixture',str(ROOT/'fixture.json'),
                   '--rate','.0125','--step-delay','1.2','--startup-concurrency','2',
                   '--mode','follower','--output',str(output), *CASES[name]]
        started = time.time()
        print(json.dumps({'case':name,'status':'start','command':command}), flush=True)
        with (ROOT/'results'/f'{name}.log').open('w') as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        print(json.dumps({'case':name,'status':'finished','exit_code':result.returncode,'elapsed_s':time.time()-started}), flush=True)
        if (output/'summary.json').exists():
            report=analyze(output,ROOT/'probe')
            print(json.dumps({'case':name,'delivery_ms':report['commit_to_receipt_ms'],'incomplete':report['incomplete_pos_rows']}),flush=True)
        if result.returncode:
            break
