#!/usr/bin/env python3
"""Read-only reconciliation of an interrupted real-order workload."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

import psycopg2

parser=argparse.ArgumentParser()
parser.add_argument('directory',type=Path)
args=parser.parse_args()
latest={}
for line in (args.directory/'clients.jsonl').open():
    row=json.loads(line)
    if row['event']=='table_action_complete':
        latest[row['db'],row['order_id']]=row['expected']
by_db=defaultdict(dict)
for (db,oid),expected in latest.items():
    by_db[db][oid]=expected
result=[]
for db,orders in by_db.items():
    assert db.startswith('posbus_')
    with psycopg2.connect(dbname=db,user='coder',host='/var/run/postgresql') as connection:
        connection.set_session(readonly=True)
        with connection.cursor() as cr:
            for oid,expected in orders.items():
                cr.execute('SELECT uuid,state,amount_total,amount_tax,table_id FROM pos_order WHERE id=%s',(oid,))
                uuid,state,total,tax,table=cr.fetchone()
                assert uuid==expected['uuid'] and table==expected['table_id']
                cr.execute('SELECT id,qty,price_unit,price_subtotal,price_subtotal_incl FROM pos_order_line WHERE order_id=%s ORDER BY id',(oid,))
                lines=cr.fetchall()
                assert len(lines)==2
                assert abs(sum(float(r[4]) for r in lines)-float(total))<.005
                assert abs(sum(float(r[4]-r[3]) for r in lines)-float(tax))<.005
                assert all(abs(float(r[1]*r[2]-r[3]))<.005 for r in lines)
                cr.execute('SELECT l.pos_order_line_id,sum(l.quantity-l.cancelled) FROM pos_prep_line l JOIN pos_prep_order p ON p.id=l.prep_order_id WHERE p.pos_order_id=%s GROUP BY l.pos_order_line_id',(oid,))
                prep={line:float(qty) for line,qty in cr.fetchall()}
                cr.execute('SELECT count(*),coalesce(sum(amount),0) FROM pos_payment WHERE pos_order_id=%s',(oid,))
                payments,paid=cr.fetchone()
                result.append({'db':db,'id':oid,'uuid':uuid,'state':state,'total':float(total),'tax':float(tax),'lines':[{'id':r[0],'qty':float(r[1]),'price':float(r[2])} for r in lines],'prepared_net':prep,'payments':payments,'paid':float(paid),'matches_last_acknowledged_total':abs(float(total)-expected['totals']['total'])<.005})
output={'checked_orders':len(result),'states':dict(Counter(r['state'] for r in result)),'line_total_tax_identity_checks_passed':True,'orders':result,'scope':'Reconciles persisted state after interrupted drain; does not claim all scheduled actions ran, finalization, or peer-cache convergence.'}
(args.directory/'persisted-interrupted-audit.json').write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({k:v for k,v in output.items() if k!='orders'}))
