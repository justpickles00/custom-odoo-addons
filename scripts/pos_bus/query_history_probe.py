#!/usr/bin/env python3
"""Read-only, post-load diagnostic of native Bus history exclusion overhead.

The advanced bound is a counterfactual, NOT a correctness-preserving replacement
for the native safety window. Both queries return the same latest stored rows
inside one REPEATABLE READ, READ ONLY snapshot; no Odoo server is started.

Pinned source: Odoo 5bcf314eb0896bdf7e218341a134901004ba3d4d,
addons/bus/tools/notifications.py:40-51, especially line 49's NOT IN condition;
addons/bus/bus_dispatcher.py:170-201 and :524-538 construct the history/exclusions.
Run only after load has stopped. Timings describe warm offline SQL, not the
concurrent dispatcher, Python history construction, or socket delivery.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time

import psycopg2


BASE_SELECT = """
SELECT b.id, b.message, b.channel
FROM bus_bus b
JOIN (VALUES (%s, %s)) AS v(channel, last_fetched_id)
  ON b.channel = v.channel AND b.id > v.last_fetched_id
"""
SOURCE = {
    'commit': '5bcf314eb0896bdf7e218341a134901004ba3d4d',
    'query': 'addons/bus/tools/notifications.py:40-51',
    'not_in': 'addons/bus/tools/notifications.py:49-50',
    'history': 'addons/bus/bus_dispatcher.py:170-201,524-538',
}


def compact_plan(value):
    """Retain timings/buffers/tree structure without huge literal Filter lists."""
    if isinstance(value, dict):
        return {key: compact_plan(item) for key, item in value.items()}
    if isinstance(value, list):
        return [compact_plan(item) for item in value]
    if isinstance(value, str) and len(value) > 512:
        return {'text_omitted': True, 'characters': len(value),
                'sha256': hashlib.sha256(value.encode()).hexdigest()}
    return value


def trial(cursor, channel, history, expected_ids, variant):
    if variant == 'native_history':
        query = BASE_SELECT + ' WHERE b.id NOT IN %s ORDER BY b.id'
        params = (channel, history[0] - 1, tuple(history))
    else:
        query = BASE_SELECT + ' ORDER BY b.id'
        params = (channel, history[-1])
    started = time.monotonic_ns()
    adapted = cursor.mogrify(query, params)
    adapted_at = time.monotonic_ns()
    # Execute the adapted bytes to separate adaptation from SQL execution.
    cursor.execute(adapted)
    executed_at = time.monotonic_ns()
    rows = cursor.fetchall()
    fetched_at = time.monotonic_ns()
    ids = [row[0] for row in rows]
    if ids != expected_ids:
        raise AssertionError(f'{variant}: returned IDs differ from latest sample')
    explain_started = time.monotonic_ns()
    cursor.execute(b'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + adapted)
    explain = cursor.fetchone()[0][0]
    explain_ended = time.monotonic_ns()
    plan = explain['Plan']
    return {
        'variant': variant,
        'history_size': len(history),
        'lower_bound': params[1],
        'mogrify_ms': (adapted_at - started) / 1e6,
        'query_bytes': len(adapted),
        'select_execute_ms': (executed_at - adapted_at) / 1e6,
        'select_fetch_ms': (fetched_at - executed_at) / 1e6,
        'select_total_ms': (fetched_at - started) / 1e6,
        'selected_rows': len(rows),
        'identical_ids': True,
        'explain_wall_ms': (explain_ended - explain_started) / 1e6,
        'planning_ms': explain.get('Planning Time'),
        'execution_ms': explain.get('Execution Time'),
        'plan_actual_rows': plan.get('Actual Rows'),
        'plan_actual_loops': plan.get('Actual Loops'),
        'root_buffers': {key: item for key, item in plan.items()
                         if 'Blocks' in key or key.startswith('I/O ')},
        'explain': compact_plan(explain),
    }


def resolve_channel(args):
    if args.channel:
        target = args.channel
    else:
        fixture = json.loads(args.fixture.read_text())
        if 'tenants' in fixture:
            fixture = next((item for item in fixture['tenants']
                            if item['db'] == args.db), None)
            if fixture is None:
                raise ValueError('Requested database is absent from fixture tenants')
        target = fixture.get('access_token')
        if not target:
            raise ValueError('Supply --channel or a fixture with access_token')
    return json.dumps([args.db, target], separators=(',', ':'))


def run(args):
    if not args.db.startswith('posbus_'):
        raise ValueError('Only isolated posbus_ fixture databases are allowed')
    if args.output.exists():
        raise FileExistsError(f'Output already exists: {args.output}')
    channel = resolve_channel(args)
    connection = psycopg2.connect(
        dbname=args.db, user='coder', host='/var/run/postgresql',
        application_name='posbus-query-history-probe',
        options=f'-c statement_timeout={args.statement_timeout_ms} -c lock_timeout=2000',
    )
    result = {
        'database': args.db, 'source': SOURCE,
        'channel_sha256': hashlib.sha256(channel.encode()).hexdigest(),
        'history_sizes': args.history_sizes, 'sample_rows': args.sample_rows,
        'warmup_pairs': args.warmups, 'measured_pairs': args.repetitions,
        'limitations': (
            'Advanced bound is a counterfactual, not a correctness-preserving '
            'replacement: it would miss late lower-ID commits in a live system. '
            'History sizes are controlled row counts, not measured live history. '
            'Warm offline SQL excludes concurrent load, dispatcher history/set '
            'construction, notification decoding and socket delivery. EXPLAIN '
            'instrumentation adds overhead; SELECT execution/fetch and adaptation '
            'are timed separately. Trials share warmed caches and one snapshot.'
        ),
        'trials': [],
    }
    try:
        connection.set_session(isolation_level='REPEATABLE READ', readonly=True)
        with connection.cursor() as cursor:
            cursor.execute("SELECT version(), current_setting('transaction_read_only'), "
                           "current_setting('transaction_isolation'), "
                           "txid_current_snapshot()::text, pg_backend_pid()")
            version, read_only, isolation, snapshot, pid = cursor.fetchone()
            if read_only != 'on' or isolation != 'repeatable read':
                raise AssertionError('Diagnostic requires repeatable-read read-only snapshot')
            result.update(server_version=version, read_only=read_only,
                          isolation=isolation, snapshot=snapshot, backend_pid=pid)
            cursor.execute("SELECT indexname, indexdef FROM pg_indexes "
                           "WHERE schemaname = current_schema() AND tablename = 'bus_bus' "
                           "ORDER BY indexname")
            result['indexes'] = [dict(zip(('name', 'definition'), row))
                                 for row in cursor.fetchall()]
            required = max(args.history_sizes) + args.sample_rows
            cursor.execute('SELECT id FROM bus_bus WHERE channel = %s '
                           'ORDER BY id DESC LIMIT %s', (channel, required))
            stored = sorted(row[0] for row in cursor.fetchall())
            if len(stored) != required:
                raise ValueError(f'Channel needs {required} stored rows; found {len(stored)}')
            expected = stored[-args.sample_rows:]
            result.update(expected_id_min=expected[0], expected_id_max=expected[-1],
                          expected_ids_sha256=hashlib.sha256(
                              json.dumps(expected, separators=(',', ':')).encode()).hexdigest())
            for size in args.history_sizes:
                history = stored[-(size + args.sample_rows):-args.sample_rows]
                for pair in range(args.warmups + args.repetitions):
                    variants = ('native_history', 'advanced_bound')
                    if pair % 2:
                        variants = tuple(reversed(variants))
                    for position, variant in enumerate(variants):
                        measured = trial(cursor, channel, history, expected, variant)
                        if pair >= args.warmups:
                            measured.update(repetition=pair - args.warmups,
                                            pair_position=position)
                            result['trials'].append(measured)
    finally:
        connection.rollback()
        connection.close()
    result['identical_ids_all_trials'] = True
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    return result


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='posbus_0000')
    parser.add_argument('--fixture', type=Path,
                        default=Path('/home/coder/playground/pos-bus-investigation/fixture.json'))
    parser.add_argument('--channel', help='Optional string target instead of fixture access_token')
    parser.add_argument('--history-sizes', nargs='+', type=int, default=[1000, 10000, 30000])
    parser.add_argument('--sample-rows', type=int, default=1000)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--statement-timeout-ms', type=int, default=30000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if min(*args.history_sizes, args.sample_rows, args.repetitions,
           args.statement_timeout_ms) <= 0 or args.warmups < 0:
        parser.error('Counts and timeout must be positive; warmups may be zero')
    return args


if __name__ == '__main__':
    report = run(arguments())
    print(json.dumps({key: report[key] for key in (
        'database', 'history_sizes', 'sample_rows', 'identical_ids_all_trials')}))
