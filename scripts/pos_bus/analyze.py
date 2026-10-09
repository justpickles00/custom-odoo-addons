#!/usr/bin/env python3
"""Join actual transaction commits to browser-free socket receipts by tenant/id."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path


def rows(path):
    with path.open() as stream:
        for line in stream:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # A concurrently written final line is incomplete, not an event.
                continue


def stats(values, scale=1):
    values = sorted(v / scale for v in values)
    if not values:
        return {'n': 0}
    def percentile(p):
        return values[min(len(values)-1, int((len(values)-1)*p))]
    return {'n': len(values), 'mean': sum(values)/len(values), 'p50': percentile(.5),
            'p95': percentile(.95), 'p99': percentile(.99), 'max': values[-1]}


def analyze(directory, probe):
    clients = list(rows(directory / 'clients.jsonl'))
    summary = json.loads((directory / 'summary.json').read_text())
    operations = {r['operation_id'] for r in clients if r['event'] == 'rpc_start'}
    lifecycle_ops = {r['operation_id'] for r in clients if r['event'] == 'order_step'}
    run = next(r for r in clients if r['event'] == 'run_start')
    measurement = next((r for r in clients if r['event'] == 'measurement_start'), {})
    measure_start = measurement.get('scheduled_start_ns', 0)
    measurement_end = next((r for r in clients if r['event'] == 'measurement_end'), {})
    measure_end = measurement_end.get('scheduled_end_ns', 2**63)
    start = min(r['monotonic_ns'] for r in clients)
    end = max(r['monotonic_ns'] for r in clients)
    devices = {r['device']: r for r in clients if r['event'] == 'device_started'}
    commits, dispatch, metadata = {}, {}, {}
    batches, cursor_commits, rpc_spans = [], [], []
    recorder_drops = {}
    for file in probe.glob('probe-*.jsonl'):
        for row in rows(file):
            kind = row['kind']
            if kind == 'commit_return' and row.get('operation_id') in operations:
                for bus_id in row['bus_ids']:
                    commits[row['db'], bus_id] = row
                for item in row.get('bus_rows', []):
                    metadata[row['db'], item['id']] = item
            elif kind == 'dispatch_batch' and start <= row['start_ns'] <= end:
                batches.append(row)
                for bus_id in row.get('bus_ids', []):
                    dispatch.setdefault((row['db'], bus_id), row)
            elif kind == 'cursor_commit' and row.get('operation_id') in lifecycle_ops:
                cursor_commits.append(row)
            elif kind == 'rpc_dispatch' and row.get('operation_id') in lifecycle_ops:
                rpc_spans.append(row)
            elif kind == 'recorder_stats':
                recorder_drops[row['pid']] = max(recorder_drops.get(row['pid'], 0), row['dropped_total'])
    latency = defaultdict(list)
    tenants = defaultdict(list)
    delivery_count, duplicate_count, unmatched = Counter(), 0, 0
    actual_recipients = defaultdict(set)
    seen = set()
    event_types, bytes_by_type = Counter(), Counter()
    per_event = defaultdict(list)
    for row in clients:
        if row['event'] != 'notification':
            continue
        key = row['db'], row.get('bus_id', row.get('id'))
        commit = commits.get(key)
        if not commit:
            unmatched += 1
            continue
        delivery_key = (*key, row['device'])
        if delivery_key in seen:
            duplicate_count += 1
            continue
        seen.add(delivery_key)
        delivery_count[key] += 1
        actual_recipients[key].add(row['device'])
        received = row.get('received_ns', row.get('mono_ns', row['monotonic_ns']))
        ns = received - commit['commit_return_ns']
        notification_type = row.get('notification_type', row.get('type', 'unknown'))
        group = 'pos' if notification_type.endswith('-SYNCHRONISATION') else 'canary' if notification_type == 'posbus.canary' else 'other'
        if group == 'canary' and ':ready:' in str(commit.get('operation_id')):
            group = 'canary_startup'
        elif group == 'canary' and ':final:' in str(commit.get('operation_id')):
            group = 'canary_final'
        if notification_type == 'posbus.synthetic':
            group = 'synthetic'
        latency[group].append(ns)
        tenants[row['db']].append(ns)
        per_event[key].append(ns)
        event_types[notification_type.split('-')[-1]] += 1
        bytes_by_type[group] += row.get('payload_bytes', len(json.dumps(row.get('payload')).encode()))
        if key in dispatch:
            batch = dispatch[key]
            latency[group + '_commit_to_claim'].append(batch['start_ns'] - commit['commit_return_ns'])
            latency[group + '_after_dispatch'].append(received - batch['end_ns'])
    # Server metadata establishes expectations even for a completely lost row.
    expected_pos, received_pos, missing_pos = 0, 0, []
    observed_pos = {(r['db'], r.get('bus_id', r.get('id'))) for r in clients
                    if r['event'] == 'notification' and str(r.get('notification_type', r.get('type',''))).endswith('-SYNCHRONISATION')}
    pos_keys = {key for key, value in metadata.items() if value['type'].endswith('-SYNCHRONISATION')}
    if not metadata:
        pos_keys = observed_pos
    for key in pos_keys:
        intended = {name for name,d in devices.items() if d['db'] == key[0] and d['role'] == 'till'}
        expected = len(intended)
        expected_pos += expected
        received_pos += delivery_count[key]
        if actual_recipients[key] != intended:
            missing_pos.append({'db': key[0], 'id': key[1], 'expected': expected, 'received': delivery_count[key],
                                'missing_devices':sorted(intended-actual_recipients[key]),
                                'unexpected_devices':sorted(actual_recipients[key]-intended)})
    stream_delivery = defaultdict(lambda: {'expected':0,'received':0,'rows':0,'mismatches':[]})
    for key,meta in metadata.items():
        kind=meta['type']
        if kind=='posbus.canary':
            roles={'till','background'}
            category='canary'
        elif kind.endswith('-SYNCHRONISATION') or kind.endswith('-ORDER_STATE_CHANGED') or kind=='posbus.synthetic':
            roles={'till'}
            category=kind.split('-')[-1]
        else:
            continue
        intended={name for name,d in devices.items() if d['db']==key[0] and d['role'] in roles}
        check=stream_delivery[category]
        check['rows']+=1
        check['expected']+=len(intended)
        check['received']+=len(actual_recipients[key])
        if actual_recipients[key]!=intended:
            check['mismatches'].append({'db':key[0],'id':key[1],
                'missing':sorted(intended-actual_recipients[key]),'unexpected':sorted(actual_recipients[key]-intended)})
    warm_keys = {key for key, commit in commits.items()
                 if measure_start <= commit['commit_return_ns'] <= measure_end
                 and ':final:' not in str(commit.get('operation_id'))
                 and ':ready:' not in str(commit.get('operation_id'))}
    selected_batches = [b for b in batches if any((b['db'], x) in warm_keys for x in b.get('bus_ids', []))]
    stages = ('queue_wait_max_ns', 'cursor_acquire_ns', 'session_validation_ns', 'fetch_ns',
              'dispatch_ns', 'serialize_ns', 'enqueue_ns', 'duration_ns')
    result = {'run_id': summary['run_id'], 'args': run['args'], 'counts': summary['counts'],
              'commit_to_receipt_ms': {k: stats(v, 1e6) for k,v in latency.items()},
              'by_tenant_ms': {k: stats(v, 1e6) for k,v in tenants.items()},
              'event_max_receipt_ms': stats([max(v) for v in per_event.values()], 1e6),
              'batch_stage_wall_ms': {stage: stats([b.get(stage, 0) for b in selected_batches], 1e6) for stage in stages},
              'batch_stage_total_wall_ms': {stage: sum(b.get(stage, 0) for b in selected_batches)/1e6 for stage in stages},
              'order_rpc_method_ms': stats([r['duration_ns'] for r in rpc_spans], 1e6),
              'order_cursor_commit_ms': stats([r['duration_ns'] for r in cursor_commits], 1e6),
              'notification_types': dict(event_types), 'payload_bytes_delivered': dict(bytes_by_type),
              'stream_delivery':dict(stream_delivery),
              'commit_rows_matched': len(commits), 'dispatch_batches_matched': len(selected_batches),
              'duplicate_receipts': duplicate_count, 'unmatched_receipts': unmatched,
              'observed_pos_rows_expected_deliveries': expected_pos, 'observed_pos_rows_received_deliveries': received_pos,
              'incomplete_pos_rows': missing_pos, 'recorder_drops': recorder_drops,
              'delivery_limit': 'Server commit metadata includes wholly missing POS rows.' if metadata else 'Legacy probe: observed POS rows only.',
              'delivery_window': 'All operations scheduled by this run, including drain; stage_wall uses commits inside scheduled measurement window.',
              'stage_warning': 'Stage spans are overlapping wall time, not CPU time. Commit-to-claim and receipt-minus-batch-end may be negative during overlapping fetch/send.'}
    (directory / 'analysis.json').write_text(json.dumps(result, indent=2)+'\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--probe', type=Path, default=Path('/home/coder/playground/pos-bus-investigation/probe'))
    args = parser.parse_args()
    result = analyze(args.directory, args.probe)
    print(json.dumps({k: result[k] for k in ('run_id','counts','commit_to_receipt_ms','batch_stage_wall_ms','incomplete_pos_rows','recorder_drops')}, indent=2))
