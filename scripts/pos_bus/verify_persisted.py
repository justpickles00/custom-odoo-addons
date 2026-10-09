#!/usr/bin/env python3
"""Independently verify committed order/payment/preparation state after a run."""
import argparse
from collections import defaultdict
import json
from pathlib import Path

import psycopg2


def verify(directory):
    expected = defaultdict(dict)
    for line in (directory/'clients.jsonl').read_text().splitlines():
        row=json.loads(line)
        if row['event']=='lifecycle_complete' and not row.get('synthetic'):
            expected[row['db']][row['order_id']]=row['final_state']
    checked,paid,cancelled=0,0,0
    for db,orders in expected.items():
        assert db.startswith('posbus_'), db
        with psycopg2.connect(dbname=db,user='coder',host='/var/run/postgresql') as connection:
            connection.set_session(readonly=True)
            with connection.cursor() as cr:
                cr.execute('''SELECT o.id,o.state,o.amount_total,o.amount_tax,o.amount_paid,
                     (SELECT count(*) FROM pos_order_line l WHERE l.order_id=o.id),
                     (SELECT sum(l.qty) FROM pos_order_line l WHERE l.order_id=o.id),
                     (SELECT count(*) FROM pos_payment p WHERE p.pos_order_id=o.id),
                     (SELECT sum(p.amount) FROM pos_payment p WHERE p.pos_order_id=o.id),
                     (SELECT count(*) FROM pos_prep_order p WHERE p.pos_order_id=o.id),
                     (SELECT sum(l.quantity) FROM pos_prep_line l JOIN pos_prep_order p ON p.id=l.prep_order_id WHERE p.pos_order_id=o.id)
                     FROM pos_order o WHERE o.id IN %s''',(tuple(orders),))
                rows=cr.fetchall()
                assert len(rows)==len(orders),(db,'missing orders')
                for oid,state,total,tax,tender,lines,qty,payments,paid_sum,prep,prep_qty in rows:
                    assert state==orders[oid],(db,oid,state,orders[oid])
                    assert (float(total),float(tax),lines,float(qty),prep,float(prep_qty))==(33.0,3.0,2,4.0,1,4.0),(db,oid,total,tax,lines,qty,prep,prep_qty)
                    if state=='paid':
                        assert payments==1 and float(paid_sum)==33 and float(tender)==33,(db,oid,payments,paid_sum,tender)
                        paid+=1
                    else:
                        assert state=='cancel' and payments==0,(db,oid,state,payments)
                        cancelled+=1
                    checked+=1
    result={'checked_orders':checked,'paid':paid,'cancelled':cancelled,'databases':len(expected),'passed':True}
    (directory/'persisted-verification.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('directories',type=Path,nargs='+')
    args=parser.parse_args()
    for directory in args.directories:
        print(directory.name,json.dumps(verify(directory)))
