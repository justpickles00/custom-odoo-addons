# Follower cache freshness during fast POS changes

The browser-free protocol emulator reproduced two stale intermediate peer-cache
checks during a rapid native order lifecycle, despite notification receipts and
successful follower RPCs. This is evidence about the emulated native query/cache
path, not a demonstrated browser DOM failure. Source inspection identifies a
one-second timestamp threshold that can exclude a same-second update.

Source is pinned to Odoo master
`5bcf314eb0896bdf7e218341a134901004ba3d4d`. Upstream code was not modified.

## Native query construction

In [DevicesSynchronisation.constructOrdersDomain](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/static/src/app/utils/devices_synchronisation.js:203),
an already-known record's timestamp is advanced by one second before the
follower asks the server for changes. The following is the exact source at
lines 203–219, with the closing lines included for readability:

```javascript
const recordDateTime = record.write_date.plus({ seconds: 1 }).toUTC();
const recordDateTimeString = recordDateTime.toFormat("yyyy-MM-dd HH:mm:ss", {
    numberingSystem: "latn",
});

let domain = new Domain([
    ["id", "=", record.id],
    ["write_date", ">=", recordDateTimeString],
]);

if (model === "pos.order") {
    domain = Domain.or([
        domain,
        new Domain([
            ["id", "=", record.id],
            ["state", "!=", record.state],
        ]),
    ]);
}
```

For example, a cached `write_date` of `12:00:00` produces a threshold of
`12:00:01`. A quantity-only edit whose exposed write timestamp remains
`12:00:00` and whose order state remains `draft` does not match either
condition. A state change has a separate escape through `state != cached_state`.
This example is a source-derived explanation, not a timestamp measurement from
the traces below.

[collect](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/static/src/app/utils/devices_synchronisation.js:64)
skips the originating device; other devices call
[readDataFromServer](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/static/src/app/utils/devices_synchronisation.js:94),
which builds this domain and invokes `pos.config.read_config_open_orders`.
[The server method](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/models/pos_config.py:288)
uses the supplied domain to select updated orders and then returns their related
records. Receiving the notification therefore does not itself guarantee that
the follower query returns the changed record graph.

## Correctness smoke evidence

Each run used one real fixture database, two POS tills and two background
sockets, with native persisted create/edit/preparation/finalization RPCs. The
emulator maintained related-record caches and checked the other till after each
step. No browser was started.

| Run | Step delay | Peer checks | Outcome |
| --- | ---: | ---: | --- |
| [smoke-03-caches](/home/coder/playground/pos-bus-investigation/results/smoke-03-caches/clients.jsonl), `8a675bdb4bbc` | 1.2 s | 8 | All eight passed; lifecycle ended in cancellation. |
| [smoke-04-paid-caches](/home/coder/playground/pos-bus-investigation/results/smoke-04-paid-caches/clients.jsonl), `d59263d25193` | 1.2 s | 8 | All eight passed; lifecycle ended in payment. |
| [smoke-05-fast-caches](/home/coder/playground/pos-bus-investigation/results/smoke-05-fast-caches/clients.jsonl), `1bdcdb2e966b` | 0.01 s | 8 | Six passed; two failed at the 0.4 s verification deadline. |

The fast run completed all eight originating order steps and all 16 follower
reads. All 39 client RPCs completed successfully. It recorded 56 notification
receipts, including both native synchronisation publications per step, and
passed its initial and final subscription barriers. Its two failed checks were:

- Order 31, origin till 0, `prepare`: peer till 1 had the expected `draft` state
  and quantities `[2, 2]`, but no preparation records/graph after 0.408766 s.
- Order 32, origin till 1, `edit`: peer till 0 still had quantities `[1, 2]`
  instead of `[2, 2]`, with state still `draft`, after 0.409014 s.

The [fast-run summary](/home/coder/playground/pos-bus-investigation/results/smoke-05-fast-caches/summary.json)
records two `follower_assertion_error` events and no RPC error. The second
failure is consistent with the unchanged-state/same-second predicate above;
the preparation graph failure can also result when the parent order does not
qualify for rereading. The traces do not isolate every possible cache-merge or
callback-order contribution, so the exact browser-visible cause remains to be
verified independently.

## What this establishes

The smokes distinguish transport delivery from follower state freshness.
Duplicate notifications or faster socket delivery alone cannot prove that the
query returns a change excluded by its domain. The slower smokes provide
positive checks for cancellation, payment and related preparation data under
the tested timing, while the fast smoke exposes an intermediate-state weakness
in the emulated path. Later steps may refresh a stale cache; these checks do
not establish permanent data loss.

The emulator implements a bounded subset of native protocol, domain construction,
related-record loading and cache behavior. It does not execute OWL, IndexedDB,
the full relational model, kitchen display behavior, or DOM updates. This finding
is a reason to test the timestamp/query semantics in the actual client; it is
not a Bus capacity result or a transport replacement decision.
