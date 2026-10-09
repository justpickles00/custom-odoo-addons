#!/usr/bin/env python3
"""Cancel only exact fixture order identities recorded by an interrupted run.

Dry-run by default. Stop every writer for these orders before --apply: native
RPCs do not provide an atomic compare-and-cancel operation. Authoritative server
snapshots and RPC responses are retained; no order or database is deleted.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from uuid import UUID

import aiohttp


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('owned_orders', type=Path, help='The failed run\'s owned-orders.json')
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--http-url', default='http://127.0.0.1:8061')
    parser.add_argument('--rpc-timeout', type=float, default=90)
    parser.add_argument('--output', type=Path, help='New evidence directory; must not exist')
    parser.add_argument('--apply', action='store_true', help='Cancel verified owned drafts; requires stopped writers')
    args = parser.parse_args()
    if args.rpc_timeout <= 0:
        parser.error('--rpc-timeout must be positive')
    args.output = args.output or args.owned_orders.parent / ('cleanup-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ'))
    return args


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def relation(value):
    return value[0] if isinstance(value, list) and value else value


def fixture_for(manifest, db):
    if 'tenants' in manifest:
        found = [row for row in manifest['tenants'] if row.get('db') == db]
        if len(found) != 1:
            raise ValueError('Missing or ambiguous fixture tenant: ' + db)
        return found[0]
    # Role-specific fixture templates have stable IDs across cloned databases.
    return manifest.get('restaurant', manifest)


def validate_owned(owned):
    if not isinstance(owned.get('run_id'), str) or not owned['run_id']:
        raise ValueError('owned-orders.json must identify its run_id')
    tenants = owned.get('tenants')
    if not isinstance(tenants, dict):
        raise ValueError('owned-orders.json must contain a tenants mapping')
    for db, orders in tenants.items():
        if not re.fullmatch(r'posbus_[A-Za-z0-9_.-]+', db):
            raise ValueError('Cleanup restricted to posbus_ fixture databases: ' + db)
        ids, uuids = set(), set()
        for order in orders:
            oid, uuid = order.get('id'), order.get('uuid')
            if type(oid) is not int or oid <= 0 or not isinstance(uuid, str):
                raise ValueError(f'Invalid owned identity in {db}: {order}')
            UUID(uuid)
            if oid in ids or uuid in uuids:
                raise ValueError(f'Duplicate owned identity in {db}: {oid}/{uuid}')
            ids.add(oid)
            uuids.add(uuid)


class Client:
    def __init__(self, db, fixture, args, evidence):
        self.db, self.fixture, self.args, self.evidence = db, fixture, args, evidence
        self.http = None
        self.sequence = 0

    async def rpc(self, route, params):
        self.sequence += 1
        safe_params = {**params}
        if 'password' in safe_params:
            safe_params['password'] = '[REDACTED]'
        event = {'db': self.db, 'rpc_id': self.sequence, 'route': route, 'params': safe_params}
        self.evidence.write(json.dumps({'event': 'rpc_start', **event}) + '\n')
        self.evidence.flush()
        try:
            async with self.http.post(self.args.http_url + route, json={
                'jsonrpc': '2.0', 'method': 'call', 'id': self.sequence, 'params': params,
            }) as response:
                body = await response.text()
                self.evidence.write(json.dumps({'event': 'rpc_response', **event, 'status': response.status, 'body': body}) + '\n')
                self.evidence.flush()
                os.fsync(self.evidence.fileno())
                response.raise_for_status()
                data = json.loads(body)
            if 'error' in data:
                raise RuntimeError(json.dumps(data['error']))
            return data['result']
        except Exception as error:
            self.evidence.write(json.dumps({'event': 'rpc_error', **event, 'error': str(error)}) + '\n')
            self.evidence.flush()
            raise

    async def call(self, method, args, kwargs=None):
        return await self.rpc('/web/dataset/call_kw/pos.order/' + method, {
            'model': 'pos.order', 'method': method, 'args': args, 'kwargs': kwargs or {},
        })

    async def start(self):
        self.http = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True), timeout=aiohttp.ClientTimeout(total=self.args.rpc_timeout))
        user = self.fixture['users'][0]
        result = await self.rpc('/web/session/authenticate', {
            'db': self.db, 'login': user['login'], 'password': user['password'],
        })
        if not result or result.get('uid') != user['id']:
            raise RuntimeError('Fixture-user authentication mismatch: ' + self.db)
        sessions = await self.rpc('/web/dataset/call_kw/pos.session/read', {
            'model': 'pos.session', 'method': 'read',
            'args': [[self.fixture['session_id']], ['state', 'config_id']], 'kwargs': {},
        })
        if len(sessions) != 1 or sessions[0]['state'] != 'opened' or relation(sessions[0]['config_id']) != self.fixture['config_id']:
            raise RuntimeError('Expected fixture session is not open: ' + self.db)

    async def read(self, expected):
        data = await self.call('read_pos_orders', [[['id', 'in', [row['id'] for row in expected]]]])
        actual = {row['id']: row for row in data.get('pos.order', [])}
        if set(actual) != {row['id'] for row in expected}:
            raise RuntimeError('Missing/inaccessible owned order: ' + self.db)
        for row in expected:
            order = actual[row['id']]
            if order.get('uuid') != row['uuid']:
                raise RuntimeError(f"UUID mismatch; refusing {self.db}/{row['id']}")
            for field, value in (('config_id', self.fixture['config_id']), ('session_id', self.fixture['session_id'])):
                if relation(order.get(field)) != value:
                    raise RuntimeError(f"Fixture {field} mismatch: {self.db}/{row['id']}")
            if row.get('table_id') is not None and relation(order.get('table_id')) != row['table_id']:
                raise RuntimeError(f"Owned table mismatch: {self.db}/{row['id']}")
            if order.get('state') not in ('draft', 'cancel', 'paid', 'done'):
                raise RuntimeError(f"Unexpected state: {self.db}/{row['id']}")
        return data


def order_graph(data, oid):
    order = next(row for row in data['pos.order'] if row['id'] == oid)
    batches = [row for row in data.get('pos.prep.order', []) if relation(row.get('pos_order_id')) == oid]
    ids = {row['id'] for row in batches}
    lines = [row for row in data.get('pos.prep.line', []) if relation(row.get('prep_order_id')) in ids]
    return order, batches, lines


def fingerprint(data, oid):
    order, batches, lines = order_graph(data, oid)
    return (order.get('write_date'), order['state'], sorted(row['id'] for row in batches),
            sorted((row['id'], row['quantity'], row['cancelled']) for row in lines))


async def run(args):
    owned = json.loads(args.owned_orders.read_text())
    manifest = json.loads(args.fixture.read_text())
    validate_owned(owned)
    args.output.mkdir(parents=True, exist_ok=False)
    os.chmod(args.output, 0o700)
    clients, before, outcomes = {}, {}, []
    save(args.output / 'request.json', {'run_id': owned['run_id'], 'apply': args.apply, 'owned_orders': str(args.owned_orders.resolve()), 'owned': owned})
    with (args.output / 'rpc.jsonl').open('x') as evidence:
        try:
            # Finish the entire ownership preflight and persist all snapshots
            # before making any modifying RPC, even if a later DB is invalid.
            for db, expected in owned['tenants'].items():
                if not expected:
                    continue
                client = Client(db, fixture_for(manifest, db), args, evidence)
                clients[db] = client
                await client.start()
                before[db] = await client.read(expected)
            save(args.output / 'before.json', before)
            for db, expected in owned['tenants'].items():
                if not expected:
                    continue
                client = clients[db]
                for identity in expected:
                    oid = identity['id']
                    initial, _, _ = order_graph(before[db], oid)
                    outcome = {'db': db, 'id': oid, 'uuid': identity['uuid'], 'before_state': initial['state']}
                    if initial['state'] != 'draft':
                        outcome['action'] = 'skip-' + initial['state']
                    elif not args.apply:
                        outcome['action'] = 'would-cancel'
                    else:
                        current = await client.read([identity])
                        if fingerprint(current, oid) != fingerprint(before[db], oid):
                            raise RuntimeError(f'Owned state changed since preflight; refusing {db}/{oid}')
                        order, batches, lines = order_graph(current, oid)
                        commands = []
                        for batch in batches:
                            updates = [[1, line['id'], {'cancelled': line['quantity']}] for line in lines
                                       if relation(line['prep_order_id']) == batch['id'] and line['cancelled'] != line['quantity']]
                            if updates:
                                commands.append([1, batch['id'], {'prep_line_ids': updates}])
                        context = {'config_id': client.fixture['config_id'], 'device_identifier': 0,
                                   'allowed_company_ids': [client.fixture['company_id']]}
                        if commands:
                            payload = {'uuid': order['uuid'], 'state': 'draft', 'session_id': relation(order['session_id']),
                                       'lines': [], 'payment_ids': [], 'prep_order_ids': commands}
                            await client.call('sync_from_ui', [[payload]], {'context': context})
                        prepared = await client.read([identity])
                        prepared_order, _, prepared_lines = order_graph(prepared, oid)
                        if prepared_order['state'] != 'draft' or any(line['quantity'] != line['cancelled'] for line in prepared_lines):
                            raise RuntimeError(f'Preparation cancellation verification failed: {db}/{oid}')
                        await client.call('cancel_order_from_pos', [[oid]], {'context': context})
                        final = await client.read([identity])
                        final_order, _, final_lines = order_graph(final, oid)
                        if final_order['state'] != 'cancel' or any(line['quantity'] != line['cancelled'] for line in final_lines):
                            raise RuntimeError(f'Final cancellation verification failed: {db}/{oid}')
                        outcome.update(action='cancelled', final=final)
                    outcomes.append(outcome)
                    evidence.write(json.dumps({'event': 'order_outcome', **outcome}) + '\n')
                    evidence.flush()
            result = {'passed': True, 'applied': args.apply, 'run_id': owned['run_id'], 'counts': dict(Counter(row['action'] for row in outcomes)), 'orders': outcomes}
            save(args.output / 'summary.json', result)
            print(json.dumps({key: value for key, value in result.items() if key != 'orders'}))
            print('Evidence: ' + str(args.output.resolve()))
            return 0
        except Exception as error:
            save(args.output / 'failure.json', {'passed': False, 'error': str(error), 'completed_orders': outcomes})
            raise
        finally:
            for client in clients.values():
                if client.http:
                    await client.http.close()


if __name__ == '__main__':
    raise SystemExit(asyncio.run(run(arguments())))
