#!/usr/bin/env python3
"""Independent open-loop native Bus ramp; no browsers or follower RPCs.

Producer runs in an Odoo shell with bounded threads, native _sendone (master
has no _sendmany), precommit ORM creation and normal cursor commit/NOTIFY.
Every row has a trace marker added to the small native POS sync payload. The
producer pins its own registries; this is additional load outside the server.
Its process-local commit recorder disables the optional server probe to avoid
nesting commit wrappers; the serving process instrumentation is unchanged.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
import json
import math
import os
from pathlib import Path
import queue
import threading
import time
import uuid

import aiohttp
from client import Device, Metrics
from run import barrier, fixtures_from_manifest, monitor_event_loop


def producer(spec):
    import odoo
    from odoo import api, sql_db
    from odoo.orm.registry import Registry
    from odoo.tools.lru import LRU
    from odoo.addons.bus.models.bus import BusBus

    lock = threading.Lock()
    def emit(event, **values):
        with lock:
            print('RAMP ' + json.dumps({'event': event, 'mono_ns': time.monotonic_ns(), **values}, separators=(',', ':')), flush=True)

    # Pin the native registries so DB cycling does not measure registry loading.
    cached = list(Registry.registries.items())
    Registry.registries = LRU(max(42, len(spec['fixtures']) + 2))
    for name, registry in cached:
        Registry.registries[name] = registry
    registries = {f['db']: Registry(f['db']) for f in spec['fixtures']}
    local = threading.local()
    original_create, original_commit = BusBus.create, sql_db.PsycoConnection.commit
    def create(self, values):
        records = original_create(self, values)
        if getattr(local, 'task', None) and self.env.cr is local.cursor:
            local.ids.extend(records.ids)
        return records
    def commit(connection):
        result = original_commit(connection)
        committed = time.monotonic_ns()  # Before recording or postcommit NOTIFY.
        if getattr(local, 'task', None) and connection is local.cursor._cnx:
            local.committed_ns = committed
        return result
    BusBus.create, sql_db.PsycoConnection.commit = create, commit
    queues = [queue.Queue(spec['queue_batches']) for _ in range(spec['producer_count'])]
    errors = []
    def worker(index):
        while True:
            task = queues[index].get()
            try:
                if task is None:
                    return
                fixture, case, first, count, scheduled = task
                db = fixture['db']
                started = time.monotonic_ns()
                local.ids, local.committed_ns, local.recorded = [], None, False
                with registries[db].cursor() as cr:
                    local.task, local.cursor, local.ids, local.committed_ns = task, cr, [], None
                    environment = api.Environment(cr, odoo.SUPERUSER_ID, {})
                    cr._posbus_operation_id = f"{spec['run_id']}:ramp:{case}:{db}:{first}"
                    bus = environment['bus.bus']
                    for seq in range(first, first + count):
                        payload = {'static_records': {}, 'session_id': fixture['session_id'], 'device_identifier': 'bus-ramp', 'records': {}, 'bus_ramp': {'run': spec['run_id'], 'case': case, 'seq': seq}}
                        bus._sendone(fixture['access_token'], fixture['access_token'] + '-SYNCHRONISATION', payload)
                    cr.commit()
                    ended = time.monotonic_ns()
                    if len(local.ids) != count or local.committed_ns is None:
                        raise RuntimeError('Native bus row/commit capture mismatch')
                    emit('committed', db=db, case=case, first=first, count=count, ids=local.ids, scheduled_ns=scheduled, started_ns=started, commit_ns=local.committed_ns, cursor_return_ns=ended)
                    local.recorded = True
                    local.task = None
            except BaseException as error:
                errors.append(str(error))
                if getattr(local, 'committed_ns', None) and not getattr(local, 'recorded', False):
                    fixture, case, first, count, scheduled = task
                    emit('committed', db=fixture['db'], case=case, first=first, count=len(local.ids), ids=local.ids, scheduled_ns=scheduled, started_ns=started, commit_ns=local.committed_ns, cursor_return_ns=time.monotonic_ns(), postcommit_error=repr(error))
                    local.recorded = True
                emit('producer_error', worker=index, task=str(task), error=repr(error))
                local.task = None
            finally:
                queues[index].task_done()
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(len(queues))]
    for thread in threads:
        thread.start()
    emit('producer_ready', dbs=list(registries), producer_count=len(queues))
    # Parent writes GO to a separate file only after positive socket canaries.
    control = Path(spec['control'])
    deadline = time.monotonic() + spec['startup_timeout'] * (2 if spec.get('global_go') else 1)
    while not control.exists():
        if time.monotonic() > deadline:
            raise TimeoutError('Receiver readiness control timeout')
        time.sleep(.05)
    for case, rate in enumerate(spec['rates']):
        offered, accepted, missed = 0, 0, 0
        per_db = Counter()
        start = time.monotonic_ns() + 100_000_000
        end = start + int(spec['duration'] * 1e9)
        emit('case_start', case=case, rate=rate, start_ns=start, end_ns=end)
        tick = 1
        while True:
            target_ns = min(end, start + int(tick * spec['batch_period'] * 1e9))
            time.sleep(max(0, (target_ns - time.monotonic_ns()) / 1e9))
            due = math.floor(rate * (target_ns - start) / 1e9 + 1e-8)
            previous_offered, previous_missed = offered, missed
            # Rotate admission order so a full shared queue does not always
            # favor the same tenant. Stable DB indices still assign producers.
            for offset in range(len(spec['fixtures'])):
                db_index = (tick - 1 + offset) % len(spec['fixtures'])
                fixture = spec['fixtures'][db_index]
                desired = max(0, (due + len(spec['fixtures']) - 1 - db_index) // len(spec['fixtures']))
                count = desired - per_db[fixture['db']]
                while count:
                    size = min(count, spec['batch_size'])
                    first = per_db[fixture['db']]
                    per_db[fixture['db']] += size
                    count -= size
                    offered += size
                    try:
                        queues[db_index % len(queues)].put_nowait((fixture, case, first, size, target_ns))
                        accepted += size
                    except queue.Full:
                        missed += size
            emit('offer_tick', case=case, scheduled_ns=target_ns, offered=offered - previous_offered, missed=missed - previous_missed, scheduler_lag_ns=max(0, time.monotonic_ns() - target_ns))
            if target_ns >= end:
                break
            tick += 1
        emit('offer_end', case=case, offered=offered, accepted=accepted, missed=missed, end_ns=end)
        for pending in queues:
            pending.join()
        emit('producer_drained', case=case)
        # Parent confirms receipt drain before next independent case.
        ack = Path(spec['output']) / f'case-{case}.drained'
        wait_end = time.monotonic() + spec['drain_timeout'] + 30
        while not ack.exists():
            if time.monotonic() > wait_end:
                raise TimeoutError('Parent drain acknowledgement timeout')
            time.sleep(.05)
    for pending in queues:
        pending.put(None)
    for thread in threads:
        thread.join()
    emit('producer_done', errors=len(errors))


class RampMetrics(Metrics):
    def event(self, kind, **values):
        if kind == 'ws_raw':
            return
        if kind == 'notification':
            marker = (values.get('payload') or {}).get('bus_ramp')
            if marker and marker.get('run') == self.run_id:
                self.receipts[marker['case']] += 1
                super().event('receipt', case=marker['case'], seq=marker['seq'], db=values['db'], device=values['device'], bus_id=values['bus_id'], received_ns=values['received_ns'])
                return
        super().event(kind, **values)


class RampDevice(Device):
    async def read_notifications(self):
        if not self.args.compact_receipts:
            return await super().read_notifications()
        try:
            async for frame in self.ws:
                received = time.monotonic_ns()
                if frame.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError(str(self.ws.exception()))
                if frame.type != aiohttp.WSMsgType.TEXT:
                    continue
                notifications = json.loads(frame.data)
                if not isinstance(notifications, list):
                    self.metrics.event('control_frame', device=self.name, value=notifications)
                    continue
                receipts = []
                for notification in notifications:
                    if notification.get('internal'):
                        kind, payload = notification.get('type'), notification.get('payload')
                        self.metrics.event('bus_control', device=self.name, type=kind, payload=payload)
                        if kind == 'bus/last_id_reset':
                            self.last_id = payload
                            self.seen_ids = type(self.seen_ids)((key, None) for key in self.seen_ids if key <= self.last_id)
                        elif kind == 'bus/subscription_outdated':
                            self.metrics.event('subscription_outdated', device=self.name)
                        continue
                    bus_id = notification.get('id', 0)
                    if bus_id in self.seen_ids:
                        self.metrics.event('duplicate_notification', device=self.name, db=self.db, bus_id=bus_id)
                        continue
                    self.seen_ids[bus_id] = None
                    if len(self.seen_ids) > 10000:
                        self.seen_ids.popitem(last=False)
                    self.last_id = max(self.last_id, bus_id)
                    message = notification.get('message', {})
                    payload = message.get('payload') or {}
                    if message.get('type') == 'posbus.canary' and payload.get('token') in self.canaries:
                        self.canaries[payload['token']].set()
                    marker = payload.get('bus_ramp')
                    if marker and marker.get('run') == self.args.run_id:
                        receipts.append([bus_id, marker['case'], marker['seq']])
                        self.metrics.receipts[marker['case']] += 1
                if receipts:
                    self.metrics.event('receipt_frame', device=self.name, db=self.db, received_ns=received, receipts=receipts)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.metrics.event('ws_error', device=self.name, db=self.db, error=str(error))
        finally:
            if not self.closed:
                self.metrics.event('ws_disconnect', device=self.name, db=self.db, close_code=self.ws.close_code)


def summarize(args, events, metrics):
    commits = {}
    cases = {}
    paused_names = set(getattr(metrics, 'paused_devices', []))
    paused_by_db = Counter(name.rsplit(':till:', 1)[0] for name in paused_names)
    for event in events:
        case = event.get('case')
        if event['event'] == 'case_start':
            cases[case] = {**event, 'transactions': 0, 'transactions_during_window': 0, 'committed_rows': 0, 'committed_during_window': 0, 'receipts': 0, 'paused_receipts': 0, 'healthy_receipts': 0, 'expected_paused_receipts': 0, 'duplicates': 0, 'last_receipt_ns': None, 'buckets': defaultdict(Counter)}
        elif event['event'] == 'committed':
            item = cases[case]
            item['transactions'] += 1
            item['producer_queue_wait_max_ms'] = max(item.get('producer_queue_wait_max_ms', 0), (event['started_ns'] - event['scheduled_ns']) / 1e6)
            item['commit_to_cursor_return_max_ms'] = max(item.get('commit_to_cursor_return_max_ms', 0), (event['cursor_return_ns'] - event['commit_ns']) / 1e6)
            item['postcommit_errors'] = item.get('postcommit_errors', 0) + bool(event.get('postcommit_error'))
            item['committed_rows'] += event['count']
            item['expected_paused_receipts'] += event['count'] * paused_by_db[event['db']]
            item['last_commit_ns'] = max(item.get('last_commit_ns', 0), event['commit_ns'])
            if event['commit_ns'] <= item['end_ns']:
                item['committed_during_window'] += event['count']
                item['transactions_during_window'] += 1
            item['buckets'][str((event['commit_ns'] - item['start_ns']) // 1_000_000_000)]['committed'] += event['count']
            for seq, bus_id in zip(range(event['first'], event['first'] + event['count']), event['ids']):
                commits[case, event['db'], seq] = (event['commit_ns'], bus_id)
        elif event['event'] == 'offer_tick':
            item = cases[case]
            bucket = item['buckets'][str((event['scheduled_ns'] - item['start_ns']) // 1_000_000_000)]
            bucket['offered'] += event['offered']
            bucket['missed'] += event['missed']
            item['scheduler_lag_max_ms'] = max(item.get('scheduler_lag_max_ms', 0), event['scheduler_lag_ns'] / 1e6)
        elif event['event'] == 'offer_end':
            cases[case].update({key: event[key] for key in ('offered', 'accepted', 'missed')})
    seen = defaultdict(dict)
    lag_metrics = Metrics(args.output / 'lag-histograms')
    unmatched = 0
    def receipt_rows():
        with (args.output / 'clients.jsonl').open() as stream:
            for line in stream:
                row = json.loads(line)
                if row['event'] == 'receipt':
                    yield row
                elif row['event'] == 'receipt_frame':
                    for bus_id, case, seq in row['receipts']:
                        yield {**row, 'bus_id': bus_id, 'case': case, 'seq': seq}
    for row in receipt_rows():
        case = row['case']
        commit = commits.get((case, row['db'], row['seq']))
        if commit is None or commit[1] != row['bus_id']:
            unmatched += 1
            continue
        item = cases[case]
        key = (row['db'], row['seq'])
        bit = 1 << int(row['device'].rsplit(':', 1)[1])
        if seen[case].get(key, 0) & bit:
            item['duplicates'] += 1
            continue
        seen[case][key] = seen[case].get(key, 0) | bit
        item['receipts'] += 1
        category = 'paused' if row['device'] in paused_names else 'healthy'
        item[category + '_receipts'] += 1
        item['last_receipt_ns'] = max(item['last_receipt_ns'] or 0, row['received_ns'])
        lag_ms = (row['received_ns'] - commit[0]) / 1e6
        lag_metrics.observe(f'case-{case}.commit_to_receipt', lag_ms)
        if paused_names:
            lag_metrics.observe(f'case-{case}.{category}.commit_to_receipt', lag_ms)
        item['buckets'][str((row['received_ns'] - item['start_ns']) // 1_000_000_000)]['receipts'] += 1
    hist = lag_metrics.summary()['histograms']
    lag_metrics.close()
    summaries = []
    for case, item in cases.items():
        item['actual_transaction_rate'] = item['transactions_during_window'] / args.duration
        item['mean_rows_per_transaction'] = item['committed_rows'] / item['transactions'] if item['transactions'] else 0
        item['actual_committed_rate'] = item['committed_during_window'] / args.duration
        item['actual_total_rate_including_producer_drain'] = item['committed_rows'] / max(args.duration, (item.get('last_commit_ns', item['end_ns']) - item['start_ns']) / 1e9)
        item['expected_receipts'] = item['committed_rows'] * args.tills
        item['missing_receipts'] = item['expected_receipts'] - item['receipts']
        item['expected_healthy_receipts'] = item['expected_receipts'] - item['expected_paused_receipts']
        item['missing_paused_receipts'] = item['expected_paused_receipts'] - item['paused_receipts']
        item['missing_healthy_receipts'] = item['expected_healthy_receipts'] - item['healthy_receipts']
        item['wholly_missing_rows'] = item['committed_rows'] - len(seen[case])
        item['fully_received_rows'] = sum(mask.bit_count() == args.tills for mask in seen[case].values())
        item['last_receipt_after_offer_end_s'] = (item['last_receipt_ns'] - item['end_ns']) / 1e9 if item['last_receipt_ns'] else None
        item['lag'] = hist.get(f'case-{case}.commit_to_receipt', {})
        if paused_names:
            item['paused_lag'] = hist.get(f'case-{case}.paused.commit_to_receipt', {})
            item['healthy_lag'] = hist.get(f'case-{case}.healthy.commit_to_receipt', {})
        cumulative_committed = cumulative_received = 0
        buckets = []
        for second in sorted(item['buckets'], key=int):
            bucket = item['buckets'][second]
            cumulative_committed += bucket.get('committed', 0)
            cumulative_received += bucket.get('receipts', 0)
            buckets.append({'second': int(second), **bucket, 'outstanding_receipts': cumulative_committed * args.tills - cumulative_received})
        item['buckets'] = buckets
        summaries.append(item)
    return {'run_id': args.run_id, 'args': vars(args), 'cases': summaries, 'unmatched_receipts': unmatched, 'paused_devices': sorted(paused_names), 'client_counts': dict(metrics.counts), 'client_histograms': metrics.summary()['histograms'], 'clock': 'same-host monotonic_ns; lag starts at native PsycoConnection.commit return before postcommit NOTIFY; payload trace marker added; transactions batch native _sendone rows; offered rate is global events/sec'}


async def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    args.mode, args.queue_size, args.cache_limit, args.closed_cache = 'passive', 32, 10000, 32
    args.follower_concurrency, args.coalesce_equal_sync, args.rpc_timeout = 1, False, 90
    args.ws_max_msg_size = 0
    args.background, args.restaurant_fraction = 0, .2
    fixtures = fixtures_from_manifest(json.loads(args.fixture.read_text()), args)
    if len(fixtures) != args.tenants:
        raise ValueError('Manifest has fewer tenants than requested')
    if any(not fixture['db'].startswith('posbus_') for fixture in fixtures):
        raise ValueError('Only isolated posbus_ databases are allowed')
    metrics = RampMetrics(args.output)
    metrics.run_id, metrics.receipts = args.run_id, Counter()
    metrics.paused_devices = []
    spec = {**vars(args), 'fixtures': fixtures, 'control': str(args.output / 'GO')}
    spec_path = args.output / 'producer-spec.json'
    spec_path.write_text(json.dumps(spec, default=str))
    code = f"import sys, runpy; sys.path.insert(0, {str(Path(__file__).resolve().parent)!r}); runpy.run_path({str(Path(__file__).resolve())!r}, run_name='bus_ramp_producer')['producer'](__import__('json').loads(open({str(spec_path)!r}).read()))\n"
    process = await asyncio.create_subprocess_exec(args.python, str(args.odoo_bin), 'shell', '-c', str(args.config), '-d', fixtures[0]['db'], '--no-http', '--max-cron-threads=0', '--db_maxconn=' + str(args.producer_count * 2 + 1), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=(args.output / 'producer-stderr.log').open('w'), env={key: value for key, value in os.environ.items() if key != 'POS_BUS_PROBE_DIR'})
    process.stdin.write(code.encode())
    await process.stdin.drain()
    process.stdin.close()
    events, devices, startup_tasks = [], [], []
    ready, finished = asyncio.Event(), asyncio.Event()
    failure = []
    loop_state = {'phase': 'startup'}
    paused_transports = {}
    def pause_readers(case):
        selected = devices[:args.pause_readers]
        metrics.paused_devices = [device.name for device in selected]
        newly_paused = []
        for device in selected:
            if device.name not in paused_transports:
                transport = device.ws._writer.transport
                transport.pause_reading()
                paused_transports[device.name] = transport
                newly_paused.append(device.name)
        if newly_paused:
            metrics.event('readers_paused', case=case, devices=newly_paused)
    def resume_readers(case, reason='producer_drained'):
        resumed = []
        for name, transport in list(paused_transports.items()):
            transport.resume_reading()
            resumed.append(name)
            del paused_transports[name]
        if resumed:
            metrics.event('readers_resumed', case=case, devices=resumed, reason=reason)
    producer_stream = (args.output / 'producer.jsonl').open('w', buffering=1024 * 1024)
    async def read_producer():
        while line := await process.stdout.readline():
            if not line.startswith(b'RAMP '):
                continue
            event = json.loads(line[5:])
            events.append(event)
            if event['event'] == 'case_start':
                loop_state['phase'] = f"case-{event['case']}"
                pause_readers(event['case'])
            elif event['event'] == 'producer_drained':
                loop_state['phase'] = f"drain-{event['case']}"
            producer_stream.write(json.dumps(event, separators=(',', ':')) + '\n')
            if event['event'] == 'producer_ready':
                ready.set()
            if event['event'] == 'producer_error':
                failure.append(event)
            if event['event'] in ('case_start', 'offer_end', 'producer_drained', 'producer_done', 'producer_error'):
                print(json.dumps(event), flush=True)
            if event['event'] == 'producer_drained':
                case = event['case']
                resume_readers(case)
                expected = sum(e['count'] for e in events if e['event'] == 'committed' and e['case'] == case) * args.tills
                deadline = time.monotonic() + args.drain_timeout
                while metrics.receipts[case] < expected and time.monotonic() < deadline:
                    await asyncio.sleep(.1)
                metrics.event('case_receipt_drain', case=case, expected=expected, actual=metrics.receipts[case], timeout=metrics.receipts[case] < expected)
                (args.output / f'case-{case}.drained').touch()
        ready.set()
        finished.set()
    reader = asyncio.create_task(read_producer())
    async def status():
        while True:
            await asyncio.sleep(5)
            print(json.dumps({'progress': 'bus-ramp', 'receipts': dict(metrics.receipts), 'producer_events': len(events)}), flush=True)
            metrics.flush()
    status_task = asyncio.create_task(status())
    loop_task = asyncio.create_task(monitor_event_loop(metrics, loop_state))
    try:
        await asyncio.wait_for(ready.wait(), args.startup_timeout)
        if process.returncode is not None or finished.is_set():
            raise RuntimeError('Producer exited during startup; see producer-stderr.log')
        semaphore = asyncio.Semaphore(args.startup_concurrency)
        async def start_device(device):
            async with semaphore:
                await device.start()
        for fixture in fixtures:
            for index in range(args.tills):
                device = RampDevice(fixture, f"{fixture['db']}:till:{index}", 'till', args, metrics)
                device.till_index = index
                devices.append(device)
        startup_tasks = [asyncio.create_task(start_device(device)) for device in devices]
        await asyncio.gather(*startup_tasks)
        await barrier(devices, 'ramp-ready', args, metrics)
        (args.output / 'RECEIVERS_READY').touch()
        if args.global_go is not None:
            metrics.event('global_gate_wait', path=str(args.global_go))
            gate_deadline = time.monotonic() + args.startup_timeout
            while not args.global_go.exists():
                if time.monotonic() >= gate_deadline:
                    raise TimeoutError('Global readiness gate timeout')
                await asyncio.sleep(.05)
            metrics.event('global_gate_passed', path=str(args.global_go))
        pause_readers(0)
        Path(spec['control']).touch()
        await asyncio.wait_for(finished.wait(), len(args.rates) * (args.duration + args.drain_timeout + 45) + 60)
        code = await process.wait()
        if code:
            raise RuntimeError(f'Producer exited {code}; see producer-stderr.log')
        await barrier(devices, 'ramp-final', args, metrics)
    finally:
        resume_readers(None, reason='cleanup')
        for task in startup_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*startup_tasks, return_exceptions=True)
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 10)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        reader.cancel()
        status_task.cancel()
        loop_task.cancel()
        await asyncio.gather(reader, status_task, loop_task, return_exceptions=True)
        await asyncio.gather(*(device.close() for device in devices), return_exceptions=True)
        metrics.flush()
        producer_stream.close()
        summary = summarize(args, events, metrics)
        (args.output / 'summary.json').write_text(json.dumps(summary, default=str, indent=2) + '\n')
        metrics.close()
        print(json.dumps({'output': str(args.output), 'cases': [{k: v for k, v in case.items() if k != 'buckets'} for case in summary['cases']]}, default=str), flush=True)
    bad_clients = ('duplicate_notification', 'ws_error', 'ws_disconnect', 'decode_error', 'subscription_outdated', 'missing_canary')
    return int(bool(failure) or summary['unmatched_receipts'] or any(metrics.counts[key] for key in bad_clients) or any(case['missing_receipts'] or case['missed'] for case in summary['cases']))


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--odoo-bin', type=Path, default=Path('/home/coder/worktrees/justpickles00/odoo/master/odoo-bin'))
    parser.add_argument('--python', default='/home/coder/venvs/odoo/master/bin/python')
    parser.add_argument('--http-url', default='http://127.0.0.1:8061')
    parser.add_argument('--ws-url', default='http://127.0.0.1:8062')
    parser.add_argument('--db-prefix', default='posbus_')
    parser.add_argument('--db-start', type=int, default=0)
    parser.add_argument('--tenants', type=int, default=20)
    parser.add_argument('--tills', type=int, default=4)
    parser.add_argument('--rates', type=float, nargs='+', default=[20, 100, 500, 2000, 10000])
    parser.add_argument('--duration', type=float, default=20)
    parser.add_argument('--producer-count', type=int, choices=range(1, 5), default=4)
    parser.add_argument('--batch-period', type=float, default=.1, help='Open-loop enqueue cadence seconds; each DB receives its share; transaction cap below')
    parser.add_argument('--batch-size', type=int, default=1000, help='Maximum event rows per native commit; 1 gives per-event commit baseline')
    parser.add_argument('--global-go', type=Path, help='After warm canary, write OUTPUT/RECEIVERS_READY and wait for this shared gate file (first case only)')
    parser.add_argument('--pause-readers', type=int, default=0, help='Diagnostic: pause transport reads on first N sockets after warm canary; resume after each producer drain')
    parser.add_argument('--compact-receipts', action='store_true', help='Group exact receipts per websocket frame; omit per-payload reserialization')
    parser.add_argument('--queue-batches', type=int, default=16, help='Bound pending batches per producer; misses are counted and never retried')
    parser.add_argument('--startup-concurrency', type=int, default=2)
    parser.add_argument('--startup-timeout', type=float, default=180)
    parser.add_argument('--barrier-timeout', type=float, default=30)
    parser.add_argument('--drain-timeout', type=float, default=30)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if min(args.tenants, args.tills, args.duration, args.batch_period, args.batch_size, args.queue_batches, args.startup_concurrency, *args.rates) <= 0:
        parser.error('Counts, rates, durations and batches must be positive')
    if not 0 <= args.pause_readers < args.tenants * args.tills:
        parser.error('pause-readers must be nonnegative and smaller than the total receiver count')
    if args.batch_size > 1000:
        parser.error('batch-size is capped at1000 to bound pipe record size and memory')
    args.output = args.output.resolve()
    if args.global_go is not None:
        args.global_go = args.global_go.resolve()
    args.run_id = uuid.uuid4().hex[:12]
    return args


if __name__ == '__main__':
    raise SystemExit(asyncio.run(run(arguments())))
