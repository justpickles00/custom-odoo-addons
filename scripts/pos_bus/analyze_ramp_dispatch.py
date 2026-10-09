#!/usr/bin/env python3
"""Join native ramp commits to server dispatch batches, one cohort at a time.

No client log is read. Commit times come from producer.jsonl's actual native
PsycoConnection.commit return, and probe bus_ids identify the rows fetched by a
server dispatch batch. Probe records must be flushed before this offline run.
"""
from __future__ import annotations

import argparse
from array import array
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import sys


STAGES = ('duration_ns', 'claim_ns', 'cursor_acquire_ns',
          'session_validation_ns', 'fetch_ns', 'dispatch_ns', 'forward_ns',
          'serialize_ns', 'enqueue_ns')


def stats(values):
    if not values:
        return {'count': 0}
    ordered = sorted(values)
    def percentile(fraction):
        return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]
    return {'count': len(ordered), 'mean_ms': math.fsum(values) / len(values),
            'p50_ms': percentile(.5), 'p95_ms': percentile(.95),
            'p99_ms': percentile(.99), 'min_ms': ordered[0],
            'max_ms': ordered[-1], 'negative_count': sum(value < 0 for value in values),
            'above_100ms': sum(value > 100 for value in values),
            'above_1000ms': sum(value > 1000 for value in values),
            'above_5000ms': sum(value > 5000 for value in values)}


def json_rows(path):
    with path.open() as stream:
        for number, line in enumerate(stream, 1):
            try:
                yield json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f'{path}:{number}: incomplete or invalid JSON; flush/stop writers first') from error


def analyze_cohort(producer_path, probe_files, aggregate_lags):
    # Values share the large integer commit clock across all rows in a txn.
    # Mutating compact lists avoids a second half-million-key dispatch index.
    commits = {}
    cases = {}
    transaction_count = committed_events = duplicate_commit_ids = 0
    for row in json_rows(producer_path):
        event = row.get('event')
        if event == 'case_start':
            cases[row['case']] = {'case': row['case'], 'rate': row['rate'],
                                  'start_ns': row['start_ns'], 'end_ns': row['end_ns']}
        elif event == 'committed':
            transaction_count += 1
            committed_events += len(row['ids'])
            if len(row['ids']) != row['count']:
                raise ValueError(f'{producer_path}: committed count and native IDs differ')
            db = sys.intern(row['db'])
            for bus_id in row['ids']:
                key = (db, bus_id)
                if key in commits:
                    duplicate_commit_ids += 1
                    if commits[key][0] != row['commit_ns']:
                        raise ValueError(f'{producer_path}: conflicting commit clocks for {key}')
                    continue
                commits[key] = [row['commit_ns'], None, None, row['case']]
    batch_counts, stage_sums, stage_maxima = Counter(), Counter(), Counter()
    recorder_drops = {}
    recorder_stages = {}
    malformed_probe_lines = 0
    time_buckets = defaultdict(Counter)
    origin_ns = min((case['start_ns'] for case in cases.values()), default=0)
    for value in commits.values():
        time_buckets[(value[0] - origin_ns) // 1_000_000_000]['committed_rows'] += 1
    for path in probe_files:
        with path.open() as stream:
            for line_number, line in enumerate(stream, 1):
                # Most HTTP probe records are irrelevant and can be very large.
                if '"dispatch_batch"' not in line and '"recorder_stats"' not in line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    # Coverage must expose an incomplete final probe line.
                    malformed_probe_lines += 1
                    continue
                if row.get('kind') == 'recorder_stats':
                    pid = str(row['pid'])
                    recorder_drops[pid] = max(recorder_drops.get(pid, 0), row.get('dropped_total', 0))
                    recorder_stages[pid] = row.get('stages_enabled')
                    continue
                if row.get('kind') != 'dispatch_batch':
                    continue
                db = row['db']
                matched = []
                for bus_id in row.get('bus_ids', []):
                    value = commits.get((db, bus_id))
                    if value is not None:
                        matched.append(value)
                if not matched:
                    continue
                batch_counts['matching_batches'] += 1
                batch_counts['matching_row_occurrences'] += len(matched)
                if row.get('rollback'):
                    batch_counts['rollback_batches'] += 1
                    batch_counts['rollback_row_occurrences'] += len(matched)
                    continue
                batch_counts['successful_matching_batches'] += 1
                batch_counts['successful_matching_row_occurrences'] += len(matched)
                batch_counts['websocket_send_calls'] += row.get('websocket_send_calls', 0)
                batch_counts['serialized_bytes'] += row.get('serialized_bytes', 0)
                for stage in STAGES:
                    if stage in row:
                        stage_sums[stage] += row[stage]
                        stage_maxima[stage] = max(stage_maxima[stage], row[stage])
                for value in matched:
                    if value[1] is None or row['start_ns'] < value[1]:
                        value[1] = row['start_ns']
                    if value[2] is None or row['end_ns'] < value[2]:
                        value[2] = row['end_ns']
    per_case = {}
    lags = {'commit_to_batch_start': array('d'), 'commit_to_batch_end': array('d')}
    for case_id, case in cases.items():
        per_case[case_id] = {**case, 'committed_rows': 0, 'matched_rows': 0,
                             'batch_end_after_offer_end_rows': 0,
                             'lags': {name: array('d') for name in lags}}
    matched_rows = 0
    for value in commits.values():
        case = per_case.setdefault(value[3], {'case': value[3], 'committed_rows': 0,
                    'matched_rows': 0, 'batch_end_after_offer_end_rows': 0,
                    'lags': {name: array('d') for name in lags}})
        case['committed_rows'] += 1
        if value[2] is None:
            continue
        matched_rows += 1
        case['matched_rows'] += 1
        case['batch_end_after_offer_end_rows'] += value[2] > case.get('end_ns', 2**63)
        time_buckets[(value[2] - origin_ns) // 1_000_000_000]['first_successful_batch_end_rows'] += 1
        for name, index in (('commit_to_batch_start', 1), ('commit_to_batch_end', 2)):
            milliseconds = (value[index] - value[0]) / 1e6
            lags[name].append(milliseconds)
            case['lags'][name].append(milliseconds)
    for case in per_case.values():
        case['unmatched_rows'] = case['committed_rows'] - case['matched_rows']
        case['lags'] = {name: stats(values) for name, values in case['lags'].items()}
    for name, values in lags.items():
        aggregate_lags[name].extend(values)
    outstanding, buckets = 0, []
    for second, counts in sorted(time_buckets.items()):
        outstanding += counts['committed_rows'] - counts['first_successful_batch_end_rows']
        buckets.append({'second': second, **counts, 'committed_without_batch_end': outstanding})
    return {'producer': str(producer_path), 'transactions': transaction_count,
            'committed_event_records': committed_events, 'committed_unique_rows': len(commits),
            'duplicate_commit_ids': duplicate_commit_ids, 'matched_rows': matched_rows,
            'unmatched_rows': len(commits) - matched_rows,
            'matched_fraction': matched_rows / len(commits) if commits else None,
            'lags': {name: stats(values) for name, values in lags.items()},
            'cases': list(per_case.values()), 'batches': dict(batch_counts),
            'selected_batch_stage_sums_ms': {name: value / 1e6 for name, value in stage_sums.items()},
            'selected_batch_stage_max_ms': {name: value / 1e6 for name, value in stage_maxima.items()},
            'time_bucket_origin_ns': origin_ns, 'time_buckets': buckets,
            'probe_recorder_dropped_total_by_pid': recorder_drops,
            'probe_recorder_stages_enabled_by_pid': recorder_stages,
            'malformed_matching_probe_lines': malformed_probe_lines}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path, help='Cohort root containing producer.jsonl files')
    parser.add_argument('--probe', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    producers = sorted(root.rglob('producer.jsonl'))
    probe_files = sorted(args.probe.resolve().glob('probe-*.jsonl'))
    if not producers:
        parser.error('No producer.jsonl files found below root')
    if not probe_files:
        parser.error('No probe-*.jsonl files found')
    lags = {'commit_to_batch_start': array('d'), 'commit_to_batch_end': array('d')}
    results = []
    for producer_path in producers:
        print(json.dumps({'analyzing': str(producer_path), 'probe_files': len(probe_files)}), flush=True)
        results.append(analyze_cohort(producer_path, probe_files, lags))
    dropped = {}
    for result in results:
        for pid, count in result['probe_recorder_dropped_total_by_pid'].items():
            dropped[pid] = max(dropped.get(pid, 0), count)
    total_rows = sum(result['committed_unique_rows'] for result in results)
    matched_rows = sum(result['matched_rows'] for result in results)
    summary = {'root': str(root), 'probe': str(args.probe.resolve()),
        'probe_files': [str(path) for path in probe_files],
        'cohort_count': len(results), 'committed_unique_rows': total_rows,
        'matched_rows': matched_rows, 'unmatched_rows': total_rows - matched_rows,
        'matched_fraction': matched_rows / total_rows if total_rows else None,
        'lags': {name: stats(values) for name, values in lags.items()},
        'probe_recorder_dropped_total_by_pid': dropped,
        'cohorts': results,
        'interpretation': [
            'Same-host monotonic clocks join each actual persisted (db,bus_id) to producer native commit return.',
            'Only dispatch batches containing those exact bus IDs are selected; canaries and unrelated runs are excluded.',
            'Earliest successful batch start and end are minimized independently for rows fetched more than once; rollback batches are counted but excluded from delivery lag.',
            'Batch end occurs after native dispatch/enqueue and claim release/history work. It is not a socket write or client receipt timestamp, and this join proves row fetch/batch completion rather than individual recipient delivery.',
            'Batch start precedes fetch; a row committed during a running batch can legitimately have negative commit-to-batch-start lag. Values are retained without clipping.',
            'Stage sums include overlapping and nested timing scopes. They are measured wall-time sums, not additive CPU time; shared batches can appear in more than one cohort.',
            'Missing probe rows, recorder drops, disabled stages or incomplete probe lines reduce coverage; unmatched commits are reported explicitly.',
            'Each cohort index is discarded before the next; only double arrays of aggregate row lags remain for exact percentiles.',
        ]}
    output = (args.output or root / 'server-delivery-analysis.json').resolve()
    output.write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({'output': str(output), 'committed_unique_rows': total_rows,
                      'matched_rows': matched_rows, 'unmatched_rows': total_rows - matched_rows,
                      'lags': summary['lags']}), flush=True)


if __name__ == '__main__':
    main()
