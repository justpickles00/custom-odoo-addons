#!/usr/bin/env python3
"""Sustained browser-free native POS workload with long-lived table orders.

Schedules are wall-clock offered actions, with one serial writer per till and a
bounded queue. Followers retain the existing native notification/query emulator;
this does not execute OWL, IndexedDB, printer I/O, or a kitchen display. It is a
controlled protocol workload, not an independently measured human arrival trace.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
import json
from pathlib import Path
import time
import uuid

import aiohttp

from client import Device, Metrics
from orders import Order
from run import barrier, fixtures_from_manifest, monitor_event_loop, periodic_canary


class TableOrder(Order):
    """Keep line identities; native preparation batches contain quantity deltas."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.edit_count = 0
        self.prep_batches = []
        self.preparation_count = 0

    def edit_table(self):
        self.edit_count += 1
        line = self.lines[(self.edit_count - 1) % len(self.lines)]
        # Bounded basket sizes avoid hundreds of products on a long-lived table.
        line['qty'] = 1 + (self.edit_count // len(self.lines)) % 4
        line['customer_note'] = f'Table service request {self.edit_count}: serve together'
        return self._payload(dirty_lines=[line])

    def prepare_table(self, cancelled=False):
        payload = self._payload(state=self.state)
        commands, new_lines = [], []
        if cancelled:
            # Native updateLastOrderChange({cancelled: true}) cancels every
            # previously prepared unit, without first preparing unsent changes.
            for batch in self.prep_batches:
                updates = []
                for part in batch['lines']:
                    if part['cancelled'] != part['qty']:
                        part['cancelled'] = part['qty']
                        updates.append([1, part['id'], {'cancelled': part['cancelled']}])
                if updates:
                    commands.append([1, batch['id'], {'prep_line_ids': updates}])
            payload['prep_order_ids'] = commands
            return payload
        for index, line in enumerate(self.lines):
            sent = sum(part['qty'] - part['cancelled'] for batch in self.prep_batches for part in batch['lines'] if part['line_index'] == index)
            delta = line['qty'] - sent
            if delta > 0:
                role = f'prep-{self.preparation_count}-line-{index}'
                new_lines.append({'uuid': self._uuid(role), 'line_index': index,
                                  'qty': delta, 'cancelled': 0, 'id': None})
            elif delta < 0:
                remaining = -delta
                for batch in reversed(self.prep_batches):
                    updates = []
                    for part in batch['lines']:
                        if part['line_index'] != index:
                            continue
                        cancelled = min(part['qty'] - part['cancelled'], remaining)
                        if cancelled:
                            part['cancelled'] += cancelled
                            remaining -= cancelled
                            updates.append([1, part['id'], {'cancelled': part['cancelled']}])
                    if updates:
                        commands.append([1, batch['id'], {'prep_line_ids': updates}])
                    if not remaining:
                        break
                assert remaining == 0
        if new_lines:
            batch = {'uuid': self._uuid(f'prep-{self.preparation_count}'), 'id': None, 'lines': new_lines}
            self.prep_batches.append(batch)
            commands.append([0, 0, {'uuid': batch['uuid'], 'prep_line_ids': [[0, 0, {
                'uuid': part['uuid'], 'product_id': self.lines[part['line_index']]['product']['id'],
                'pos_order_line_id': self.lines[part['line_index']]['id'],
                'quantity': part['qty'], 'cancelled': 0, 'attribute_value_ids': [],
            }] for part in new_lines]}])
        self.preparation_count += 1
        payload['prep_order_ids'] = commands
        return payload

    def accept(self, response, step):
        expected = 'paid' if step == 'pay' else 'cancel' if step == 'cancel' else self.state
        row = self.apply_response(response)
        assert row['state'] == expected, (row['state'], expected)
        for key, total in [('amount_total', self.totals['total']), ('amount_tax', self.totals['tax'])]:
            assert abs(row[key] - total) < .005, (key, row[key], total)
        lines = {row['uuid']: row for row in response.get('pos.order.line', [])}
        if step != 'cancel':
            for line in self.lines:
                assert line['uuid'] in lines, ('missing line', line['uuid'])
                assert lines[line['uuid']]['qty'] == line['qty']
        prep_orders = {row['uuid']: row for row in response.get('pos.prep.order', [])}
        prep_lines = {row['uuid']: row for row in response.get('pos.prep.line', [])}
        for batch in self.prep_batches:
            if batch['uuid'] in prep_orders:
                batch['id'] = prep_orders[batch['uuid']]['id']
            assert batch['id']
            for part in batch['lines']:
                if part['uuid'] in prep_lines:
                    actual = prep_lines[part['uuid']]
                    part['id'] = actual['id']
                    assert actual['quantity'] == part['qty']
                    assert actual['cancelled'] == part['cancelled']
                assert part['id']
        if step == 'pay':
            assert self.payment_id and abs(row['amount_paid'] - self.totals['total']) < .005

    def expected(self):
        return {'id': self.id, 'uuid': self.uuid, 'state': self.state,
                'table_id': self.table_id, 'totals': self.totals,
                'lines': [{'id': line['id'], 'uuid': line['uuid'], 'qty': line['qty'],
                           'price': line['product']['price']} for line in self.lines],
                'prep_batches': self.prep_batches, 'payment_id': self.payment_id}


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--http-url', default='http://127.0.0.1:8061')
    parser.add_argument('--ws-url', default='ws://127.0.0.1:8062')
    parser.add_argument('--db-prefix', default='posbus_')
    parser.add_argument('--db-start', type=int, default=0)
    parser.add_argument('--tenants', type=int, default=20)
    parser.add_argument('--restaurant-fraction', type=float, default=.2)
    parser.add_argument('--tills', type=int, default=4)
    parser.add_argument('--tables-per-till', type=int, default=2)
    parser.add_argument('--background', type=int, default=2)
    parser.add_argument('--duration', type=float, default=180)
    parser.add_argument('--think-time', type=float, default=2, help='Wall-clock action interval per table; not response plus think time')
    parser.add_argument('--prepare-every', type=int, default=4, help='Every nth offered table action sends preparation deltas')
    parser.add_argument('--cancel-fraction', type=float, default=.2)
    parser.add_argument('--queue-size', type=int, default=128)
    parser.add_argument('--follower-concurrency', type=int, default=8)
    parser.add_argument('--coalesce-equal-sync', action='store_true', help='COUNTERFACTUAL: coalesce equal native follower callbacks only within one websocket frame; preserve every raw receipt')
    parser.add_argument('--startup-concurrency', type=int, default=12)
    parser.add_argument('--cache-limit', type=int, default=10000)
    parser.add_argument('--closed-cache', type=int, default=64)
    parser.add_argument('--rpc-timeout', type=float, default=60)
    parser.add_argument('--barrier-timeout', type=float, default=60)
    parser.add_argument('--drain-timeout', type=float, default=120)
    parser.add_argument('--canary-interval', type=float, default=30)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    for name in ('tenants', 'tills', 'tables_per_till', 'duration', 'think_time', 'prepare_every', 'queue_size', 'follower_concurrency', 'startup_concurrency', 'cache_limit', 'rpc_timeout', 'barrier_timeout', 'drain_timeout'):
        if getattr(args, name) <= 0:
            parser.error(name + ' must be positive')
    if args.background < 0 or args.closed_cache < 0 or args.canary_interval < 0:
        parser.error('background, closed cache and canary interval must be nonnegative')
    if not 0 < args.restaurant_fraction <= 1 or not 0 <= args.cancel_fraction <= 1:
        parser.error('invalid fraction')
    args.mode = 'follower'
    args.run_id = uuid.uuid4().hex[:12]
    args.output = args.output or Path('/home/coder/playground/pos-bus-results') / ('restaurant-' + args.run_id)
    return args


async def transition(device, order, step, scheduled_ns, metrics, sequence):
    operation = f'{device.args.run_id}:{device.name}:table:{order.sequence}:{step}:{sequence}'
    started = time.monotonic_ns()
    metrics.event('table_action_start', device=device.name, db=device.db, operation_id=operation,
                  order_id=order.id, table_id=order.table_id, step=step, scheduled_ns=scheduled_ns,
                  scheduling_lag_ns=max(0, started - scheduled_ns))
    metrics.observe('table_action_queue_lag', max(0, started - scheduled_ns) / 1e6)
    if step == 'cancel':
        result = await device.call('pos.order', 'cancel_order_from_pos', [[order.id]], {'context': order.context(device.identifier)}, operation)
    else:
        if step == 'prepare':
            # checkPreparationStateAndSentOrderInPreparation reads even when
            # no new preparation quantities will need synchronizing.
            rows = await device.call('pos.order', 'read', [[order.id], ['write_date']], operation_id=operation + ':check')
            if len(rows) != 1 or rows[0]['write_date'] != order.write_date:
                raise RuntimeError(f'Unexpected external write before preparation: {order.id}')
        payload = (order.edit_table() if step == 'edit' else
                   order.prepare_table(cancelled=step == 'prepare_cancel') if step in ('prepare', 'prepare_cancel') else
                   getattr(order, step)())
        # Native syncAllOrders filters orders with no dirty fields. Notes have
        # already been sent by edit actions; a note-only preparation adds no RPC.
        if step in ('prepare', 'prepare_cancel') and not payload['prep_order_ids']:
            result = None
            metrics.event('preparation_noop', device=device.name, db=device.db, order_id=order.id, step=step)
        else:
            result = await device.call('pos.order', 'sync_from_ui', [[payload]], {'context': order.context(device.identifier)}, operation)
    if result is not None:
        order.accept(result, step)
        await device.missing_recursive(result)
    device.prune_closed()
    metrics.observe('table_action.' + step, (time.monotonic_ns() - started) / 1e6)
    metrics.event('table_action_complete', device=device.name, db=device.db, operation_id=operation,
                  order_id=order.id, table_id=order.table_id, step=step, sequence=sequence,
                  elapsed_ns=time.monotonic_ns() - started, expected=order.expected())


async def writer(device, queue, metrics):
    while True:
        order, step, sequence, scheduled_ns = await queue.get()
        try:
            if device.failed:
                metrics.event('table_action_abandoned', device=device.name, db=device.db,
                              order_id=order.id, step=step, sequence=sequence, scheduled_ns=scheduled_ns,
                              reason='writer_failed', source='queued')
                continue
            device.writer_active = True
            await transition(device, order, step, scheduled_ns, metrics, sequence)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            device.failed = True
            metrics.event('table_action_error', device=device.name, db=device.db, order_id=order.id, step=step, error=str(error),
                          fail_stop=True, reconciliation_required=True)
        finally:
            device.writer_active = False
            queue.task_done()


async def schedule_table(device, order, queue, start, phase, args, metrics):
    sequence = 0
    while True:
        deadline = start + phase * args.think_time + sequence * args.think_time
        if deadline >= start + args.duration:
            return
        await asyncio.sleep(max(0, deadline - time.monotonic()))
        sequence += 1
        step = 'prepare' if sequence % args.prepare_every == 0 else 'edit'
        metrics.event('table_action_scheduled', device=device.name, db=device.db, order_id=order.id,
                      table_id=order.table_id, step=step, sequence=sequence, scheduled_ns=int(deadline * 1e9))
        if device.failed:
            metrics.event('table_action_abandoned', device=device.name, db=device.db,
                          order_id=order.id, step=step, sequence=sequence, scheduled_ns=int(deadline * 1e9),
                          reason='writer_failed', source='future_schedule')
            continue
        try:
            queue.put_nowait((order, step, sequence, int(deadline * 1e9)))
        except asyncio.QueueFull:
            metrics.event('table_schedule_dropped', device=device.name, db=device.db, order_id=order.id, step=step, sequence=sequence)


async def sample_load(devices, queues, state, metrics):
    previous = Counter(metrics.counts)
    while True:
        await asyncio.sleep(1)
        counts = metrics.counts
        rate = {name: counts[name] - previous[name] for name in ('table_action_scheduled', 'table_action_complete', 'table_schedule_dropped', 'table_action_error', 'follower_complete')}
        previous = Counter({name: counts[name] for name in rate})
        backlog = sum(queue.qsize() + int(device.writer_active) for device, queue in queues.items())
        metrics.event('load_sample', phase=state['phase'], backlog=backlog,
                      follower_backlog=sum(device.notifications.qsize() + device.follow_active for device in devices),
                      cached_draft_order_copies=sum(row.get('state') == 'draft' for device in devices if device.role == 'till' for row in device.cache['pos.order'].values()),
                      backlog_by_till={device.name: queue.qsize() + int(device.writer_active) for device, queue in queues.items()},
                      interval_counts=rate)
        metrics.flush()
        if counts['load_sample'] % 5 == 0:
            print(json.dumps({'phase': state['phase'], 'elapsed_s': round(time.monotonic() - state['start'], 1), 'backlog': backlog, 'interval_counts': rate}), flush=True)


def verify_persisted(expected, output):
    """SQL reads independently check actual IDs, totals, taxes and prep graph."""
    import psycopg2
    checked = paid = cancelled = 0
    for db, orders in expected.items():
        if not db.startswith('posbus_'):
            raise ValueError('SQL verification restricted to posbus_ fixture databases')
        with psycopg2.connect(dbname=db, user='coder', host='/var/run/postgresql') as connection:
            connection.set_session(readonly=True)
            with connection.cursor() as cursor:
                for order in orders:
                    cursor.execute('SELECT state,amount_total,amount_tax,amount_paid,table_id FROM pos_order WHERE id=%s AND uuid=%s', (order['id'], order['uuid']))
                    state, total, tax, tender, table = cursor.fetchone()
                    assert state == order['state'] and table == order['table_id']
                    assert abs(float(total) - order['totals']['total']) < .005
                    assert abs(float(tax) - order['totals']['tax']) < .005
                    cursor.execute('SELECT id,uuid,qty,price_unit,price_subtotal,price_subtotal_incl FROM pos_order_line WHERE order_id=%s', (order['id'],))
                    lines = {row[0]: row for row in cursor.fetchall()}
                    assert len(lines) == len(order['lines'])
                    for line in order['lines']:
                        _, actual_uuid, qty, price, subtotal, inclusive = lines[line['id']]
                        assert actual_uuid == line['uuid'] and float(qty) == line['qty'] and float(price) == line['price']
                        assert abs(float(subtotal) - line['qty'] * line['price']) < .005
                        assert float(inclusive) >= float(subtotal)
                    assert abs(sum(float(row[5] - row[4]) for row in lines.values()) - order['totals']['tax']) < .005
                    cursor.execute('SELECT id,amount FROM pos_payment WHERE pos_order_id=%s', (order['id'],))
                    payments = cursor.fetchall()
                    if state == 'paid':
                        assert len(payments) == 1 and payments[0][0] == order['payment_id']
                        assert abs(float(payments[0][1]) - float(total)) < .005 and abs(float(tender) - float(total)) < .005
                        paid += 1
                    else:
                        assert state == 'cancel' and not payments
                        cancelled += 1
                    cursor.execute('SELECT p.id,p.uuid,l.id,l.uuid,l.pos_order_line_id,l.quantity,l.cancelled FROM pos_prep_order p JOIN pos_prep_line l ON l.prep_order_id=p.id WHERE p.pos_order_id=%s', (order['id'],))
                    prep = cursor.fetchall()
                    parts = {part['id']: (batch, part) for batch in order['prep_batches'] for part in batch['lines']}
                    assert len(prep) == len(parts)
                    net = Counter()
                    for batch_id, batch_uuid, part_id, part_uuid, line_id, qty, removed in prep:
                        batch, part = parts[part_id]
                        assert batch_id == batch['id'] and batch_uuid == batch['uuid'] and part_uuid == part['uuid']
                        assert line_id == order['lines'][part['line_index']]['id']
                        assert float(qty) == part['qty'] and float(removed) == part['cancelled']
                        net[line_id] += float(qty - removed)
                    assert all(net[line['id']] == (0 if state == 'cancel' else line['qty']) for line in order['lines'])
                    checked += 1
    result = {'passed': True, 'checked_orders': checked, 'paid': paid, 'cancelled': cancelled, 'databases': len(expected)}
    (output / 'persisted-verification.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


async def run(args):
    fixtures = fixtures_from_manifest(json.loads(args.fixture.read_text()), args)
    if len(fixtures) != args.tenants:
        raise ValueError('fewer fixture tenants than requested')
    for fixture in fixtures:
        if fixture.get('restaurant', True) and len(fixture['tables']) < args.tills * args.tables_per_till:
            raise ValueError('not enough distinct tables per restaurant')
    metrics = Metrics(args.output)
    state = {'phase': 'startup', 'start': time.monotonic()}
    devices, orders, queues, startup, writers, schedules = [], defaultdict(list), {}, [], [], []
    phase_tasks = []
    sampler = loop_monitor = None
    verification = None
    try:
        metrics.event('run_start', run_id=args.run_id, profile='sustained-long-lived-restaurant', native=not args.coalesce_equal_sync, args=vars(args),
                      setup_policy='Creation and finalization serial within each database; databases parallel. Only sustained actions are measured; one concurrent writer per till.')
        semaphore = asyncio.Semaphore(args.startup_concurrency)
        async def preflight(fixture):
            if not fixture.get('restaurant', True):
                return
            async with semaphore:
                device = Device(fixture, fixture['db'] + ':preflight', 'background', args, metrics)
                try:
                    device.http = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True), timeout=aiohttp.ClientTimeout(total=args.rpc_timeout))
                    user = fixture['users'][0]
                    info = await device.rpc('/web/session/authenticate', {'db': fixture['db'], 'login': user['login'], 'password': user['password']})
                    if not info or not info.get('uid'):
                        raise RuntimeError('Preflight authentication failed: ' + fixture['db'])
                    table_ids = [table['id'] for table in fixture['tables'][:args.tills * args.tables_per_till]]
                    occupied = await device.call('pos.order', 'search_read', [[['state', '=', 'draft'], ['table_id', 'in', table_ids]]],
                                                 {'fields': ['id', 'uuid', 'table_id', 'config_id'], 'context': {'allowed_company_ids': [fixture['company_id']]}})
                    metrics.event('table_occupancy_preflight', db=fixture['db'], table_ids=table_ids, occupied=occupied)
                    if occupied:
                        raise RuntimeError(f"Intended tables have retained draft orders in {fixture['db']}: {occupied}")
                finally:
                    await device.close()
        # Complete every read-only occupancy check before device registration
        # or order creation: native sync can merge a new UUID into a table draft.
        preflights = [asyncio.create_task(preflight(fixture)) for fixture in fixtures]
        phase_tasks.extend(preflights)
        await asyncio.gather(*preflights)
        async def start_device(device):
            async with semaphore:
                await device.start()
        for fixture in fixtures:
            active = args.tills if fixture.get('restaurant', True) else 0
            for index in range(active + args.background):
                role = 'till' if index < active else 'background'
                device = Device(fixture, f"{fixture['db']}:{role}:{index}", role, args, metrics)
                device.till_index, device.writer_active, device.failed = index, False, False
                devices.append(device)
        startup = [asyncio.create_task(start_device(device)) for device in devices]
        await asyncio.gather(*startup)
        await barrier(devices, 'ready', args, metrics)
        active = [device for device in devices if device.role == 'till']
        state['phase'] = 'open-tables'
        async def open_tables(device):
            for index in range(args.tables_per_till):
                order = TableOrder(device.fixture, f'{args.run_id}-{device.name}', index,
                                   device.till_index * args.tables_per_till + index,
                                   device.fixture['users'][device.till_index % len(device.fixture['users'])]['id'])
                orders[device].append(order)
                await transition(device, order, 'create', time.monotonic_ns(), metrics, 'initial')
                await transition(device, order, 'prepare', time.monotonic_ns(), metrics, 'initial')
        async def per_database_phase(action):
            groups = defaultdict(list)
            for device in active:
                groups[device.db].append(device)
            async def serial_group(group):
                for device in group:
                    await action(device)
            tasks = [asyncio.create_task(serial_group(group)) for group in groups.values()]
            phase_tasks.extend(tasks)
            await asyncio.gather(*tasks)
        metrics.event('setup_start', measured=False, phase='open-tables', serial_within_db=True)
        await per_database_phase(open_tables)
        metrics.event('setup_complete', measured=False, phase='open-tables')
        await asyncio.wait_for(asyncio.gather(*(device.notifications.join() for device in devices)), args.drain_timeout)
        if not active:
            raise ValueError('no active restaurants in selected fixture manifest')
        state['phase'] = 'load'
        measurement_counts = Counter(metrics.counts)
        start = time.monotonic() + .1
        metrics.event('measurement_start', run_id=args.run_id, scheduled_start_ns=int(start * 1e9),
                      active_tills=len(active), open_orders=sum(map(len, orders.values())), offered_actions_per_s=len(active) * args.tables_per_till / args.think_time)
        for till_index, device in enumerate(active):
            queues[device] = asyncio.Queue(args.queue_size)
            writers.append(asyncio.create_task(writer(device, queues[device], metrics)))
            for table_index, order in enumerate(orders[device]):
                phase = (table_index + till_index / len(active)) / args.tables_per_till
                schedules.append(asyncio.create_task(schedule_table(device, order, queues[device], start, phase, args, metrics)))
        sampler = asyncio.create_task(sample_load(devices, queues, state, metrics))
        loop_monitor = asyncio.create_task(monitor_event_loop(metrics, state))
        if args.canary_interval:
            by_db = defaultdict(list)
            for device in devices:
                by_db[device.db].append(device)
            schedules.extend(asyncio.create_task(periodic_canary(group, index, len(by_db), start, args, metrics)) for index, group in enumerate(by_db.values()))
        await asyncio.sleep(max(0, start + args.duration - time.monotonic()))
        metrics.event('measurement_end', run_id=args.run_id, scheduled_end_ns=int((start + args.duration) * 1e9),
                      backlog=sum(queue.qsize() + int(device.writer_active) for device, queue in queues.items()),
                      measured_counts={name: metrics.counts[name] - measurement_counts[name] for name in ('table_action_scheduled', 'table_action_complete', 'table_schedule_dropped', 'table_action_error')},
                      measured_rates_per_s={name: (metrics.counts[name] - measurement_counts[name]) / args.duration for name in ('table_action_scheduled', 'table_action_complete', 'table_schedule_dropped', 'table_action_error')})
        await asyncio.gather(*schedules)
        state['phase'] = 'drain'
        try:
            await asyncio.wait_for(asyncio.gather(*(queue.join() for queue in queues.values())), args.drain_timeout)
        except asyncio.TimeoutError:
            metrics.event('work_drain_timeout', queued=sum(queue.qsize() for queue in queues.values()),
                          active_writers=sum(device.writer_active for device in active))
            # Drop work which has not started. Allow the current RPC on each
            # till to finish; aborting it could leave an unknown committed state.
            for device, queue in queues.items():
                while not queue.empty():
                    order, step, sequence, scheduled_ns = queue.get_nowait()
                    metrics.event('table_action_abandoned', device=device.name, db=device.db,
                                  order_id=order.id, step=step, sequence=sequence, scheduled_ns=scheduled_ns)
                    queue.task_done()
            await asyncio.wait_for(asyncio.gather(*(queue.join() for queue in queues.values())), args.rpc_timeout + 5)
        for task in writers:
            task.cancel()
        await asyncio.gather(*writers, return_exceptions=True)
        state['phase'] = 'finalize'
        async def finalize(device):
            if device.failed:
                raise RuntimeError('writer failed for ' + device.name)
            for index, order in enumerate(orders[device]):
                cancel = ((device.till_index * 19 + index * 37) % 100) < args.cancel_fraction * 100
                if cancel:
                    await transition(device, order, 'prepare_cancel', time.monotonic_ns(), metrics, 'final')
                    await transition(device, order, 'cancel', time.monotonic_ns(), metrics, 'final')
                else:
                    # Explicitly submit outstanding kitchen changes before
                    # payment. Native sync ignores preparation edits once paid.
                    await transition(device, order, 'prepare', time.monotonic_ns(), metrics, 'final-before-pay')
                    await transition(device, order, 'pay', time.monotonic_ns(), metrics, 'final')
                    await transition(device, order, 'prepare', time.monotonic_ns(), metrics, 'final-after-pay')
                metrics.event('table_finalized', device=device.name, db=device.db, expected=order.expected())
        metrics.event('setup_start', measured=False, phase='finalize', serial_within_db=True)
        await per_database_phase(finalize)
        metrics.event('setup_complete', measured=False, phase='finalize')
        await barrier(devices, 'final', args, metrics)
        await asyncio.wait_for(asyncio.gather(*(device.notifications.join() for device in devices)), args.drain_timeout)
        expected = defaultdict(list)
        for device, table_orders in orders.items():
            expected[device.db].extend(order.expected() for order in table_orders)
        for device in active:
            snapshot = []
            for order in expected[device.db]:
                actual = device.cache['pos.order'].get(order['id'])
                # Native cancelled orders may be removed; paid orders in the
                # current session should retain their final state and totals.
                correct = (not actual or actual.get('state') == 'cancel') if order['state'] == 'cancel' else bool(
                    actual and actual.get('state') == 'paid'
                    and abs(actual.get('amount_total', -1) - order['totals']['total']) < .005
                    and abs(actual.get('amount_tax', -1) - order['totals']['tax']) < .005)
                snapshot.append({'order_id': order['id'], 'expected_state': order['state'],
                                 'actual': actual, 'matches': correct})
                metrics.event('final_cache_assertion' if correct else 'final_cache_assertion_error',
                              device=device.name, db=device.db, order_id=order['id'], expected_state=order['state'], actual=actual)
            metrics.event('final_cache_snapshot', device=device.name, db=device.db, orders=snapshot)
        (args.output / 'expected-orders.json').write_text(json.dumps(expected, indent=2) + '\n')
        state['phase'] = 'verify'
        verification = await asyncio.to_thread(verify_persisted, expected, args.output)
        await devices[0].rpc('/pos_bus_probe/flush', {})
        metrics.event('run_complete', run_id=args.run_id, verification=verification)
    except Exception as error:
        metrics.event('run_error', error=str(error))
        raise
    finally:
        for task in [*startup, *phase_tasks, *schedules, *writers, sampler, loop_monitor]:
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in [*startup, *phase_tasks, *schedules, *writers, sampler, loop_monitor] if task), return_exceptions=True)
        await asyncio.gather(*(device.close() for device in devices), return_exceptions=True)
        owned = defaultdict(list)
        for device, table_orders in orders.items():
            owned[device.db].extend(order.expected() for order in table_orders if order.id)
        (args.output / 'owned-orders.json').write_text(json.dumps({
            'run_id': args.run_id,
            'description': 'Owned order identities and local state for reconciliation. On failure, local values may include an unacknowledged mutation; inspect committed state independently.',
            'tenants': owned,
        }, indent=2) + '\n')
        summary = {'run_id': args.run_id, 'profile': 'sustained-long-lived-restaurant', 'native': not args.coalesce_equal_sync,
                   'elapsed_s': time.monotonic() - state['start'], 'verification': verification,
                   'limitations': 'Protocol/cache emulator; no OWL, IndexedDB, DOM, kitchen display or printer I/O. Offered schedule is controlled; one serial writer per till, bounded queue, no retries. The first writer error stops further mutations on that till and abandons its queued/future work; the failed action may have committed and needs independent reconciliation. This does not emulate native offline dirty-state retry.', **metrics.summary()}
        (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        metrics.close()
        print(json.dumps({'output': str(args.output), **summary}), flush=True)
    bad = ('table_action_error', 'table_schedule_dropped', 'rpc_error', 'ws_disconnect', 'notification_queue_full', 'follower_error', 'missing_canary', 'monitor_canary_error', 'final_cache_assertion_error', 'work_drain_timeout', 'table_action_abandoned')
    return int(any(summary['counts'].get(name) for name in bad))


if __name__ == '__main__':
    raise SystemExit(asyncio.run(run(arguments())))
