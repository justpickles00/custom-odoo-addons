#!/usr/bin/env python3
"""Clone this experiment's offline template; never touch other Odoo databases."""
import argparse
import json
import shutil
import time
from pathlib import Path

import psycopg2
from psycopg2 import sql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, required=True)
    parser.add_argument('--root', type=Path, default=Path('/home/coder/playground/pos-bus-investigation'))
    args = parser.parse_args()
    assert 1 <= args.count <= 100
    connection = psycopg2.connect(dbname='postgres', user='coder', host='/var/run/postgresql')
    connection.autocommit = True
    started = time.monotonic()
    created = []
    with connection.cursor() as cr:
        cr.execute("SELECT count(*) FROM pg_stat_activity WHERE datname='posbus_template'")
        assert cr.fetchone()[0] == 0, 'Template must be offline'
        for i in range(args.count):
            name = f'posbus_{i:04d}'
            cr.execute('SELECT 1 FROM pg_database WHERE datname=%s', (name,))
            if not cr.fetchone():
                cr.execute(sql.SQL('CREATE DATABASE {} TEMPLATE posbus_template').format(sql.Identifier(name)))
                created.append(name)
                source = args.root / 'data/filestore/posbus_template'
                if source.exists():
                    shutil.copytree(source, args.root / 'data/filestore' / name)
            print(json.dumps({'database': name, 'elapsed_s': round(time.monotonic()-started, 2)}), flush=True)
        cr.execute("SELECT count(*),sum(pg_database_size(oid)) FROM pg_database WHERE datname ~ '^posbus_[0-9]+$'")
        count, size = cr.fetchone()
    record = {'created': created, 'count': count, 'bytes': int(size), 'elapsed_s': time.monotonic()-started}
    (args.root / 'results' / f'provision-{args.count}.json').write_text(json.dumps(record, indent=2))
    connection.close()


if __name__ == '__main__':
    main()
