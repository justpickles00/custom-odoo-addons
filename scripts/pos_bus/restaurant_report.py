#!/usr/bin/env python3
"""Read-only restaurant experiment report; JSON goes to stdout.

Consumes saved analysis.json, resource-analysis.json, clients.jsonl and summary.json.
No RPCs, database connections, analyses that write files, or server starts occur.
Use --include-actions for operation-level bus row identities and payload sizes.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

from analyze import stats


def rows(path):
    # Reject malformed/truncated evidence instead of silently claiming completeness.
    with path.open() as stream:
        for number, line in enumerate(stream, 1):
            if line.strip():
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f'{path}:{number}: invalid JSON evidence') from error


def load(path):
    return json.loads(path.read_text())


def timestamp(row):
    return row.get('monotonic_ns', row.get('mono_ns', 0))


def windows(clients):
    pending, result = None, []
    for row in clients:
        if row['event'] == 'measurement_start':
            if pending is not None:
                raise ValueError('overlapping/unclosed measurement_start')
            pending = row
        elif row['event'] == 'measurement_end':
            if pending is None:
                raise ValueError('measurement_end without measurement_start')
            result.append((pending, row))
            pending = None
    if pending or not result:
        raise ValueError('complete measurement_start/end evidence required')
    return result


def compact_delivery(delivery):
    return {kind: {**{k: v for k, v in value.items() if k != 'mismatches'},
                   'mismatch_rows': len(value.get('mismatches', [])),
                   'mismatch_examples': value.get('mismatches', [])[:3]}
            for kind, value in delivery.items()}


def follower_refreshes(clients, devices, start, end):
    """Classify callback suppression using saved message/recipient identities."""
    notifications = {(r['device'], r.get('bus_id', r.get('id'))): r
                     for r in clients if r['event'] == 'notification'}
    sessions = defaultdict(set)
    for device, row in devices.items():
        if row.get('session_id') is not None:
            sessions[device].add(str(row['session_id']))
    # Older device_started records omit session_id. A notification from this
    # recipient's own unique identifier establishes its session from evidence.
    for row in notifications.values():
        device = devices.get(row['device'], {})
        payload = row.get('payload') or {}
        if (str(row.get('notification_type', row.get('type', ''))).endswith('-SYNCHRONISATION')
                and isinstance(payload, dict) and payload.get('session_id') is not None
                and device.get('device_identifier') is not None
                and str(payload.get('device_identifier')) == str(device['device_identifier'])):
            sessions[row['device']].add(str(payload['session_id']))

    def classify(row):
        if row is None:
            return 'missing_notification_evidence'
        kind = str(row.get('notification_type', row.get('type', '')))
        if not kind.endswith('-SYNCHRONISATION'):
            return 'non_synchronisation'
        device = devices.get(row['device'], {})
        if device.get('role') != 'till':
            return 'non_till_recipient'
        payload = row.get('payload') or {}
        if (not isinstance(payload, dict) or device.get('device_identifier') is None
                or payload.get('device_identifier') is None):
            return 'unknown_identity_or_session'
        if str(payload.get('device_identifier')) == str(device['device_identifier']):
            return 'same_sender_skip'
        known_sessions = sessions[row['device']]
        if len(known_sessions) != 1 or payload.get('session_id') is None:
            return 'unknown_identity_or_session'
        if str(payload['session_id']) not in known_sessions:
            return 'other_session_skip'
        return 'same_session_other_sender_refresh'

    coalesced, eligible_notifications, potential_repeats = Counter(), Counter(), Counter()
    frame_keys = set()
    for row in clients:
        if not start <= timestamp(row) < end:
            continue
        if row['event'] == 'coalesced_follower_callback':
            coalesced[classify(notifications.get((row['device'], row['bus_id'])))] += 1
        elif row['event'] == 'notification':
            classification = classify(row)
            eligible_notifications[classification] += 1
            if not str(row.get('notification_type', row.get('type', ''))).endswith('-SYNCHRONISATION'):
                continue
            received = row.get('received_ns', row.get('mono_ns'))
            if received is None:
                potential_repeats['missing_frame_identity'] += 1
                continue
            # All notifications decoded from one websocket frame share the
            # receive timestamp, matching client's frame_callbacks lifetime.
            key = (row['device'], received, row.get('notification_type', row.get('type')),
                   json.dumps(row.get('payload'), sort_keys=True, separators=(',', ':')))
            if key in frame_keys:
                potential_repeats[classification] += 1
            frame_keys.add(key)
    return {'coalesced_callbacks': sum(coalesced.values()),
            'coalesced_classification': dict(coalesced),
            'eligible_read_config_open_orders_requests_suppressed': coalesced['same_session_other_sender_refresh'],
            'coalesced_sender_skips_without_read_request': coalesced['same_sender_skip'],
            'potential_equal_sync_repeats_in_frames': dict(potential_repeats),
            'potential_eligible_read_requests_suppressible': potential_repeats['same_session_other_sender_refresh'],
            'notification_recipient_classification': dict(eligible_notifications),
            'tills_with_unambiguous_session_evidence': sum(d['role'] == 'till' and len(sessions[name]) == 1 for name, d in devices.items()),
            'tills_without_unambiguous_session_evidence': sum(d['role'] == 'till' and len(sessions[name]) != 1 for name, d in devices.items())}


def process_role(proc, pids):
    command = proc.get('cmd', proc.get('command', ''))
    if '/bin/bash' in command:
        return None
    if 'odoo-bin gevent' in command:
        return 'gevent'
    if 'odoo-bin shell' in command:
        return 'producer'
    if 'restaurant_load.py' in command or 'bus_ramp.py' in command:
        return 'client'
    if 'postgres:' in command:
        return 'postgres'
    if 'odoo-bin' in command:
        return 'odoo_worker_http_or_cron' if proc.get('ppid') in pids else 'odoo_master'
    return None


def memory_bytes(sample, name):
    memory = sample.get('memory', '')
    if isinstance(memory, str):
        for line in memory.splitlines():
            if line.startswith(name + ':'):
                return int(line.split()[1]) * 1024
    elif isinstance(memory, dict) and name in memory:
        value = memory[name]
        return int(str(value).split()[0]) * (1024 if 'kB' in str(value) else 1)
    if name == 'MemAvailable' and 'mem_available' in sample:
        return int(str(sample['mem_available']).split()[0]) * 1024
    return None


def resources(samples, start, end):
    selected = [r for r in samples if start <= r['mono_ns'] < end]
    cpu, rss, seen = defaultdict(list), defaultdict(list), defaultdict(set)
    births, exits = Counter(), Counter()
    previous = None
    for row in selected:
        pids = {p['pid'] for p in row['processes'] if 'odoo-bin' in p.get('cmd', p.get('command', ''))}
        current = {p['pid']: (p, process_role(p, pids)) for p in row['processes']}
        resident = Counter()
        for pid, (proc, role) in current.items():
            if role:
                seen[role].add(pid)
                resident[role] += proc.get('rss', proc.get('rss_bytes', 0))
        for role, value in resident.items():
            rss[role].append(value)
        if previous:
            before, old = previous
            elapsed = (row['mono_ns'] - before['mono_ns']) / 1e9
            if elapsed <= 0:
                raise ValueError('telemetry timestamps must be strictly increasing')
            totals = Counter({role: 0.0 for _, role in current.values() if role})
            for pid, (proc, role) in current.items():
                if not role:
                    continue
                if pid not in old:
                    births[role] += 1
                    continue
                old_proc, old_role = old[pid]
                ticks = proc.get('ticks', proc.get('cpu_ticks', 0)) - old_proc.get('ticks', old_proc.get('cpu_ticks', 0))
                if ticks >= 0 and old_role == role:
                    totals[role] += ticks / row.get('ticks_per_second', row.get('clock_ticks', 100)) / elapsed * 100
            for pid, (_, role) in old.items():
                if role and pid not in current:
                    exits[role] += 1
            for role, value in totals.items():
                cpu[role].append(value)
        previous = row, current
    minima = {}
    for name in ('MemAvailable', 'MemFree', 'SwapFree'):
        values = [value for r in selected if (value := memory_bytes(r, name)) is not None]
        minima[name + '_bytes'] = min(values) if values else None
    return {'samples': len(selected), 'sample_start_ns': selected[0]['mono_ns'] if selected else None,
            'sample_end_ns': selected[-1]['mono_ns'] if selected else None,
            'host_memory_minima': minima,
            'cpu_percent_one_core': {k: stats(v) for k, v in cpu.items()},
            'role_rss_bytes': {k: {'min': min(v), 'max': max(v)} for k, v in rss.items()},
            'pid_churn': {k: {'unique_pids': len(v), 'appeared_after_first_sample': births[k],
                              'disappeared_before_last_sample': exits[k],
                              'sampled_counts_are_lower_bounds': True} for k, v in seen.items()}}


def report(directory, probe, telemetry, include_actions=False):
    summary = load(directory / 'summary.json')
    analysis = load(directory / 'analysis.json')
    resource_analysis = load(directory / 'resource-analysis.json')
    clients = list(rows(directory / 'clients.jsonl'))
    measurement_windows = windows(clients)
    run_id = summary['run_id']
    if analysis.get('run_id') != run_id:
        raise ValueError('analysis.json run_id does not match summary.json')
    run_start = next(r for r in clients if r['event'] == 'run_start')
    devices = {r['device']: r for r in clients if r['event'] == 'device_started'}
    rpc_starts = {r['operation_id']: r for r in clients if r['event'] == 'rpc_start'}
    action_starts = {r['operation_id']: r for r in clients if r['event'] == 'table_action_start'}
    completions = {r['operation_id']: r for r in clients if r['event'] == 'table_action_complete'}
    def action_owner(operation):
        while operation:
            if operation in action_starts:
                return operation
            operation = operation.rpartition(':')[0]
        return None

    receipts = defaultdict(set)
    for row in clients:
        if row['event'] == 'notification':
            receipts[row['db'], row.get('bus_id', row.get('id'))].add(row['device'])
    commits, recorder_drops, metadata_errors = {}, {}, 0
    committed_operations = set()
    probe_files = sorted(probe.glob('probe-*.jsonl')) if probe.is_dir() else [probe]
    if not probe_files:
        raise ValueError('no probe evidence found')
    run_begin, run_end = min(map(timestamp, clients)), max(map(timestamp, clients))
    for path in probe_files:
        for row in rows(path):
            kind = row.get('kind')
            if kind == 'commit_return' and row.get('operation_id') in rpc_starts:
                committed_operations.add(row['operation_id'])
                for bus_id in row.get('bus_ids', []):
                    metadata = next((m for m in row.get('bus_rows', []) if m['id'] == bus_id), None)
                    commits[row['db'], bus_id] = (row, metadata)
            elif kind == 'recorder_stats' and run_begin <= timestamp(row) <= run_end:
                recorder_drops[row['pid']] = max(recorder_drops.get(row['pid'], 0), row.get('dropped_total', 0))
            elif kind == 'metadata_error' and run_begin <= timestamp(row) <= run_end:
                metadata_errors += 1
    samples = sorted(rows(telemetry), key=lambda r: r['mono_ns'])
    counts = Counter(summary.get('counts', {}))
    for name, count in Counter(r['event'] for r in clients).items():
        counts[name] = max(counts[name], count)
    bad_names = ('error', 'dropped', 'queue_full', 'timeout', 'abandoned', 'missing_canary', 'disconnect', 'eviction', 'depth_limit', 'rejected', 'denied')
    failures = {k: v for k, v in counts.items() if v and any(part in k for part in bad_names)}
    blockers = [f'{name}={count}' for name, count in sorted(failures.items())]
    if not counts['run_complete']:
        blockers.append('run_complete missing')
    if not (summary.get('verification') or {}).get('passed'):
        blockers.append('persisted verification missing/failed')
    if not counts['final_cache_assertion']:
        blockers.append('final cache assertions missing')
    finalized = Counter(r['db'] for r in clients if r['event'] == 'table_finalized')
    expected_cache_assertions = sum(finalized[d['db']] for d in devices.values() if d['role'] == 'till')
    if counts['final_cache_assertion'] < expected_cache_assertions:
        blockers.append('final cache assertions incomplete')
    unfinished_rpcs = counts['rpc_start'] - counts['rpc_end'] - counts['rpc_error']
    if unfinished_rpcs:
        blockers.append(f'RPCs without end/error={unfinished_rpcs}')
    if any(recorder_drops.values()) or any(analysis.get('recorder_drops', {}).values()):
        blockers.append('probe recorder dropped evidence')
    if metadata_errors:
        blockers.append('probe metadata errors')
    if not commits:
        blockers.append('no matching bus commits')
    missing_metadata = sum(meta is None for _, meta in commits.values())
    if missing_metadata:
        blockers.append(f'committed bus rows without metadata={missing_metadata}')
    if any(value.get('mismatches') for value in analysis.get('stream_delivery', {}).values()):
        blockers.append('saved full-run recipient mismatches')
    bus_by_action = defaultdict(list)
    for key, (commit, meta) in commits.items():
        owner = action_owner(commit['operation_id'])
        if owner:
            bus_by_action[owner].append((key, meta))
    committed_actions = {owner for op in committed_operations if (owner := action_owner(op))}
    phase_counts, operation_phases = defaultdict(Counter), {}
    for operation, action in action_starts.items():
        suffix = operation.rsplit(':', 1)[-1]
        phase = 'setup' if suffix == 'initial' else 'finalize' if suffix == 'final' else (
            'measurement' if any(b['scheduled_start_ns'] <= timestamp(action) < e['scheduled_end_ns'] for b, e in measurement_windows) else 'drain')
        phase_counts[phase]['committed_bus_rows'] += len(bus_by_action[operation])
        phase_counts[phase]['actions_with_commit_evidence'] += operation in committed_actions
        operation_phases[operation] = phase
    active_action = {}
    for row in clients:
        if row['event'] == 'table_action_start':
            active_action[row['device']] = row['operation_id']
            phase_counts[operation_phases[row['operation_id']]]['started'] += 1
        elif row['event'] == 'table_action_complete':
            phase_counts[operation_phases[row['operation_id']]]['completed_by_run_end'] += 1
        elif row['event'] == 'preparation_noop' and row['device'] in active_action:
            phase_counts[operation_phases[active_action[row['device']]]]['preparation_noops'] += 1
    ready_dbs = {r['db'] for r in clients if r['event'] == 'barrier_passed' and r.get('phase') == 'ready'}
    expected_ready_dbs = {d['db'] for d in devices.values()}
    if ready_dbs != expected_ready_dbs:
        blockers.append('startup ready barrier incomplete')
    output_windows = []
    for index, (begin, finish) in enumerate(measurement_windows):
        start, end = begin['scheduled_start_ns'], finish['scheduled_end_ns']
        if end <= start:
            raise ValueError('measurement window must have positive duration')
        duration = (end - start) / 1e9
        inside = [r for r in clients if start <= timestamp(r) < end]
        measured = Counter(r['event'] for r in inside)
        started = {op: r for op, r in action_starts.items() if start <= timestamp(r) < end}
        done = {op: r for op, r in completions.items() if start <= timestamp(r) < end}
        offered = measured['table_action_scheduled']
        dropped = measured['table_schedule_dropped']
        future_abandoned = sum(r['event'] == 'table_action_abandoned'
                               and r.get('source') == 'future_schedule' for r in inside)
        queued_abandoned = sum(r['event'] == 'table_action_abandoned'
                               and r.get('source') == 'queued' for r in inside)
        actions = defaultdict(list)
        for operation, action in started.items():
            attributed = bus_by_action[operation]
            actions[action['step']].append({'operation_id': operation, 'step': action['step'],
                'commit_observed': operation in committed_actions, 'completed_in_window': operation in done,
                'completed_by_run_end': operation in completions, 'bus_rows': len(attributed),
                'rows': [{'db': key[0], 'id': key[1], **(meta or {'type': None, 'payload_bytes': None})} for key, meta in attributed]})
        by_step = {}
        for step, items in actions.items():
            types, payload = Counter(), defaultdict(list)
            for item in items:
                for meta in item['rows']:
                    types[meta['type'] or 'unknown'] += 1
                    if meta.get('payload_bytes') is not None:
                        payload[meta['type']].append(meta['payload_bytes'])
            by_step[step] = {'started': len(items), 'completed_in_window': sum(i['completed_in_window'] for i in items),
                'completed_by_run_end': sum(i['completed_by_run_end'] for i in items),
                'operations_with_commit_evidence': sum(i['commit_observed'] for i in items),
                'bus_rows_per_started_action': stats([i['bus_rows'] for i in items]),
                'committed_rows_by_type': dict(types), 'row_payload_bytes_by_type': {k: stats(v) for k, v in payload.items()},
                'bus_rows_per_started_action_by_type': {kind: stats([sum((r.get('type') or 'unknown') == kind for r in i['rows']) for i in items]) for kind in types},
                'total_payload_bytes_per_started_action': stats([sum(r.get('payload_bytes') or 0 for r in i['rows']) for i in items])}
        loads = [r for r in inside if r['event'] == 'load_sample']
        # The sustained driver creates empty queues after measurement_start.
        backlog_start = begin.get('backlog', 0 if run_start.get('profile') == 'sustained-long-lived-restaurant' else None)
        backlog_end = finish.get('backlog')
        if backlog_end is None:
            backlog_end = loads[-1]['backlog'] if loads else None
        by_method = defaultdict(list)
        for row in inside:
            if row['event'] == 'rpc_end':
                origin = rpc_starts.get(row['operation_id'], {})
                by_method['.'.join(filter(None, (origin.get('model'), row.get('method'))))].append(row['elapsed_ns'])
        delivery = defaultdict(lambda: {'rows': 0, 'expected': 0, 'received': 0, 'mismatches': []})
        for key, (commit, meta) in commits.items():
            if not meta or not start <= commit['commit_return_ns'] <= end:
                continue
            kind = meta['type']
            if kind == 'posbus.canary':
                roles, category = {'till', 'background'}, 'canary'
            elif kind.endswith(('-SYNCHRONISATION', '-ORDER_STATE_CHANGED')) or kind == 'posbus.synthetic':
                roles, category = {'till'}, kind.split('-')[-1]
            else:
                continue
            expected = {name for name, device in devices.items() if device['db'] == key[0] and device['role'] in roles}
            actual = receipts[key]
            value = delivery[category]
            value['rows'] += 1
            value['expected'] += len(expected)
            value['received'] += len(actual)
            if actual != expected:
                value['mismatches'].append({'db': key[0], 'id': key[1], 'missing': sorted(expected - actual), 'unexpected': sorted(actual - expected)})
        delivery_matches = all(not value['mismatches'] for value in delivery.values()) and bool(delivery)
        if not delivery_matches:
            blockers.append(f'window {index + 1} recipient evidence missing/mismatched')
        resource = resources(samples, start, end)
        if resource['samples'] < 2:
            blockers.append(f'window {index + 1} telemetry insufficient')
        if len(measurement_windows) == 1:
            existing_delivery = analysis.get('measurement_stream_delivery', {})
            existing_agrees = dict(delivery) == existing_delivery
            if not existing_agrees:
                blockers.append('saved measurement delivery analysis disagrees with raw evidence')
        else:
            existing_agrees = None
        window = {'window': index + 1, 'start_ns': start, 'end_ns': end, 'duration_s': duration,
            'actions': {'offered': offered, 'admitted': offered - dropped - future_abandoned, 'dropped': dropped,
                        'future_scheduled_abandoned_before_admission': future_abandoned,
                        'queued_abandoned_after_admission': queued_abandoned,
                        'started': len(started), 'completed': len(done),
                        'offered_per_s': offered / duration, 'completed_per_s': len(done) / duration,
                        'errors': measured['table_action_error'], 'preparation_noops': measured['preparation_noop'],
                        'queue_lag_ms': stats([r['scheduling_lag_ns'] for r in started.values()], 1e6),
                        'by_step': by_step},
            'queue': {'start_backlog': backlog_start, 'end_backlog': backlog_end,
                      'growth': backlog_end - backlog_start if backlog_start is not None and backlog_end is not None else None,
                      'first_sample_backlog': loads[0]['backlog'] if loads else None,
                      'peak_sample_backlog': max((r['backlog'] for r in loads), default=None),
                      'peak_follower_backlog': max((r['follower_backlog'] for r in loads), default=None)},
            'http_rpc_completion_latency_ms': {k: stats(v, 1e6) for k, v in by_method.items()},
            'client_loop_stall_ms': stats([r['lag_ms'] for r in inside if r['event'] == 'event_loop_stall']),
            'follower_capacity': {'concurrency_per_device': run_start.get('args', {}).get('follower_concurrency'),
                                  'queue_capacity_per_device': run_start.get('args', {}).get('queue_size'),
                                  'saturation_events': measured['follower_saturation'],
                                  'queue_full_events': measured['notification_queue_full'],
                                  'completed_callbacks': measured['follower_complete']},
            'follower_refreshes': follower_refreshes(clients, devices, start, end),
            'measured_cold_load_data': {'rpc_starts': sum(r['event'] == 'rpc_start' and r.get('method') == 'load_data' for r in inside),
                                      'rpc_completions': sum(r['event'] == 'rpc_end' and r.get('method') == 'load_data' for r in inside),
                                      'rpc_errors': sum(r['event'] == 'rpc_error' and r.get('method') == 'load_data' for r in inside)},
            'recipient_exact_sets': compact_delivery(delivery), 'saved_delivery_analysis_agrees': existing_agrees,
            'resources': resource}
        window['sustained_admission_without_growth'] = (not dropped and not future_abandoned and not queued_abandoned
            and not measured['table_action_error']
            and backlog_start is not None and backlog_end is not None and backlog_end <= backlog_start)
        expected_offered = begin.get('offered_actions_per_s', 0) * duration
        if expected_offered and abs(offered - expected_offered) > 1:
            blockers.append(f'window {index + 1} offered schedule incomplete: {offered}/{expected_offered:g}')
        if len(measurement_windows) == 1:
            window['commit_to_receipt_ms'] = analysis.get('measurement_commit_to_receipt_ms', {})
            window['client_loop_load_histogram'] = summary.get('histograms', {}).get('event_loop_lag.load', {})
        if include_actions:
            window['attributed_actions'] = [item for items in actions.values() for item in items]
        output_windows.append(window)
    return {'run_id': run_id, 'directory': str(directory.resolve()), 'args': run_start.get('args', {}),
            'audit': {'successful_run': not blockers, 'blockers': sorted(set(blockers)), 'failure_counts': failures,
                      'verification': summary.get('verification'), 'final_cache_assertions': counts['final_cache_assertion'],
                      'expected_final_cache_assertions_from_finalized_orders': expected_cache_assertions,
                      'unfinished_rpcs': unfinished_rpcs, 'probe_metadata_missing_rows': missing_metadata,
                      'recorder_pids_observed_in_run': len(recorder_drops),
                      'recorder_nonzero_dropped_total_by_pid_in_run': {k: v for k, v in recorder_drops.items() if v}},
            'windows': output_windows,
            'action_phases': {k: dict(v) for k, v in phase_counts.items()},
            'reused_table_action_operation_ids': {op: n for op, n in Counter(r['operation_id'] for r in clients if r['event'] == 'table_action_start').items() if n > 1},
            'readiness': {'ready_databases': len(ready_dbs), 'expected_ready_databases': len(expected_ready_dbs),
                          'open_tables_setup_completed': any(r['event'] == 'setup_complete' and r.get('phase') == 'open-tables' for r in clients),
                          'table_occupancy_preflight_databases': len({r['db'] for r in clients if r['event'] == 'table_occupancy_preflight'}),
                          'occupied_preflight_databases': sorted({r['db'] for r in clients if r['event'] == 'table_occupancy_preflight' and r.get('occupied')})},
            'existing_resource_cases': resource_analysis.get('cases', []),
            'limits': ['Measurement events use [start,end); recipient selection follows analyze.py inclusive commit boundaries and includes drain receipts.',
                       'Action bus rows are attributed by operation_id (including child operations) to actions started inside the window; commits may occur during drain. Native preparation noops can complete with zero sync RPC/bus rows; zero rows can also mean an incomplete action.',
                       'HTTP latency covers successful RPC completions in the window, including follower calls; failures/inflight RPCs are not latency samples.',
                       'Follower suppression is classified by receipt/callback time, separately from commit-window delivery metrics. Same-session other-sender sync callbacks suppress one read_config_open_orders request each; sender/other-session skips suppress none. Session inference uses own-sender saved payloads when device_started omits session_id.',
                       'Potential equal-sync repeats share device, websocket receive timestamp, type and payload. These are a hypothetical same-frame control opportunity in native runs; actual suppression requires coalesced_follower_callback evidence. Follower RPC starts can lag receipt windows.',
                       'Event-loop load histogram covers the driver load phase; stall-only samples omit lag at or below 50 ms. Histograms report upper bounds.',
                       'Telemetry PID/churn counts are sampled LOWER BOUNDS, not connection creation or restart totals. Processes living entirely between samples are omitted, including PostgreSQL backends. CPU is aggregate percent of one core; new PIDs omit first-interval ticks, so low sampled PostgreSQL CPU does not rule out expensive connection churn.',
                       'Odoo child commands do not distinguish HTTP workers from cron; their CPU/churn are combined. Host/PostgreSQL values include other VM activity.',
                       'Queue start is zero by sustained-driver construction unless explicitly recorded; end uses measurement_end or the last sample.',
                       'Browser-free protocol/cache emulator; no browser, kitchen display or printer execution.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--probe', type=Path, required=True)
    parser.add_argument('--telemetry', type=Path, required=True)
    parser.add_argument('--include-actions', action='store_true', help='Include individual operation_ids, bus row types/IDs and payload bytes')
    args = parser.parse_args()
    try:
        result = report(args.directory, args.probe, args.telemetry, args.include_actions)
    except (ValueError, KeyError, OSError, StopIteration) as error:
        parser.exit(2, f'Incomplete or invalid saved evidence: {error}\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
