#!/usr/bin/env python3
"""Clone an offline experiment template into real, isolated tenant databases."""
import argparse
import json
from pathlib import Path
import time

import psycopg2
from psycopg2 import sql


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", default="odoo_sse_exp_template")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=Path("/home/coder/playground/sse-utility-experiment/results/provision.json"))
    args = parser.parse_args()
    connection = psycopg2.connect(dbname="postgres", user="coder", host="/var/run/postgresql")
    connection.autocommit = True
    started = time.perf_counter()
    with connection.cursor() as cr:
        for index in range(1, args.count):
            database = f"odoo_sse_exp_{index:04d}"
            cr.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,))
            if not cr.fetchone():
                cr.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    sql.Identifier(database), sql.Identifier(args.template)))
            if index % 25 == 0:
                print(f"provisioned {index + 1}/{args.count}", flush=True)
        cr.execute("SELECT count(*), sum(pg_database_size(oid)) FROM pg_database WHERE datname ~ '^odoo_sse_exp_[0-9]+$'")
        count, size = cr.fetchone()
    args.output.write_text(json.dumps({"count": count, "bytes": int(size),
                                     "elapsed_s": time.perf_counter() - started}, indent=2))
    connection.close()
