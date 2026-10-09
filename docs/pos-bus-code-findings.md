# POS notification duplication: source and persisted evidence

Verified on 2026-10-09 against upstream Odoo commit `5bcf314eb0896bdf7e218341a134901004ba3d4d`. Upstream source, probe code, and database state were read only during this audit. The browser-free experiment used real authenticated POS RPCs, persisted orders, preparation records, payments, and the native bus transport. No browser execution was used.

For this installed module combination, one ordinary single-order POS RPC publishes **two identical `SYNCHRONISATION` messages and one `ORDER_STATE_CHANGED` message**. A different POS till processes both synchronisation messages and makes two `read_config_open_orders` calls. The two publications have distinct bus IDs; this is duplication at the producer, not duplicate delivery of one bus row.

## Exact producer lines

| Source location in the pinned upstream worktree | Behavior |
| --- | --- |
| [Base POS `sync_from_ui`, line 924](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/models/pos_order.py:924) | Calls `notify_synchronisation` for each returned order unless context contains a truthy `preparation`. The experiment uses ordinary POS context without that flag. |
| [Self-order override, lines 34–38](/home/coder/worktrees/justpickles00/odoo/master/addons/pos_self_order/models/pos_order.py:34) | Calls the base implementation, browses its returned orders, then calls `_send_notification` again. |
| [Self-order notification helper, line 54](/home/coder/worktrees/justpickles00/odoo/master/addons/pos_self_order/models/pos_order.py:54) | Calls `config.notify_synchronisation` again with the same current session and request device identifier. There is no check of `self_ordering_mode`, order source, or connected self-order clients here. |
| [Self-order notification helper, line 55](/home/coder/worktrees/justpickles00/odoo/master/addons/pos_self_order/models/pos_order.py:55) | Also publishes `ORDER_STATE_CHANGED` with an empty payload. |
| [Base POS cancellation, line 850](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/models/pos_order.py:850) | Native `cancel_order_from_pos` publishes its first synchronisation message. |
| [Self-order cancellation override, lines 40–45](/home/coder/worktrees/justpickles00/odoo/master/addons/pos_self_order/models/pos_order.py:40) | After base cancellation, calls the same helper for successfully cancelled orders, producing the second synchronisation and state notification. |
| [POS empty-record notification, lines 256–262](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/models/pos_config.py:256) | Both calls use default `records={}` and produce the same invalidation payload: empty static records and records, plus session and device identifier. |

`pos_self_order` is installed automatically when `pos_restaurant` is installed: [manifest line 6](/home/coder/worktrees/justpickles00/odoo/master/addons/pos_self_order/__manifest__.py:6). The fixture's configuration ID 7 has `module_pos_restaurant=true` and **`self_ordering_mode='nothing'`**, which means Disable in [the selection definition](/home/coder/worktrees/justpickles00/odoo/master/addons/pos_self_order/models/pos_config.py:35). `point_of_sale`, `pos_restaurant`, and `pos_self_order` were all confirmed installed by SQL. No configured or connected self-order client is needed for the unconditional Python override to execute. This statement concerns publication; it does not claim that an absent self-order client performs any work.

## Persisted SQL evidence, smoke-02

Evidence files are [client events](/home/coder/playground/pos-bus-investigation/results/smoke-02/clients.jsonl), [analysis](/home/coder/playground/pos-bus-investigation/results/smoke-02/analysis.json), and per-process probe JSONL under `/home/coder/playground/pos-bus-investigation/probe/`. Run ID: `172d048c5255`. Database: `posbus_0000`. Two POS tills and two background sockets participated; background sockets did not subscribe to the POS configuration channel.

The audit matched each client's completed native operation ID to the probe's `commit_return.operation_id`, then queried the exact `commit_return.bus_ids` in `bus_bus`. All 16 operations had one matching commit with bus rows. SQL returned all 48 referenced rows, IDs 87 through 134. Of these, 32 rows were synchronisations and 16 were state notifications.

The operation ID prefix in this table is `172d048c5255:posbus_0000:`. Each triple lists the two equal synchronisation rows followed by its state row.

| Operation suffix | Native method | Bus IDs |
| --- | --- | --- |
| `till:0:order:0:create` | `sync_from_ui` | 87, 88, 89 |
| `till:1:order:0:create` | `sync_from_ui` | 90, 91, 92 |
| `till:0:order:0:edit` | `sync_from_ui` | 93, 94, 95 |
| `till:0:order:0:prepare` | `sync_from_ui` | 96, 97, 98 |
| `till:1:order:0:edit` | `sync_from_ui` | 99, 100, 101 |
| `till:0:order:0:cancel` | `cancel_order_from_pos` | 102, 103, 104 |
| `till:1:order:0:prepare` | `sync_from_ui` | 105, 106, 107 |
| `till:1:order:0:cancel` | `cancel_order_from_pos` | 108, 109, 110 |
| `till:0:order:1:create` | `sync_from_ui` | 111, 112, 113 |
| `till:1:order:1:create` | `sync_from_ui` | 114, 115, 116 |
| `till:0:order:1:edit` | `sync_from_ui` | 117, 118, 119 |
| `till:1:order:1:edit` | `sync_from_ui` | 120, 121, 122 |
| `till:0:order:1:prepare` | `sync_from_ui` | 123, 124, 125 |
| `till:1:order:1:prepare` | `sync_from_ui` | 126, 127, 128 |
| `till:0:order:1:pay` | `sync_from_ui` | 129, 130, 131 |
| `till:1:order:1:pay` | `sync_from_ui` | 132, 133, 134 |

For every triple, executable audit assertions checked:

- Exactly three SQL rows belonged to the operation's recorded committed IDs.
- Exactly two types ended in `-SYNCHRONISATION` and one in `-ORDER_STATE_CHANGED`.
- The two synchronisation rows' raw `channel` strings and raw `message` strings were byte-for-byte equal.
- The state notification's decoded payload was `{}`.

For example, rows 87 and 88 shared the same channel and message. Below, `<config-token>` replaces the fixture's access token; the audit compared the actual unredacted strings.

```json
{
  "channel": ["posbus_0000", "<config-token>"],
  "message": {
    "type": "<config-token>-SYNCHRONISATION",
    "payload": {
      "static_records": {},
      "session_id": 8,
      "device_identifier": "3",
      "records": {}
    }
  }
}
```

Row 89 used the same channel, type `<config-token>-ORDER_STATE_CHANGED`, payload `{}`. Both other device identifiers and every other triple passed the same equality assertion. These SQL queries reproduce the configuration and rows while they remain retained by the native bus vacuum:

```sql
BEGIN READ ONLY;
SELECT id, name, self_ordering_mode, module_pos_restaurant
FROM pos_config WHERE id = 7;
SELECT name, state FROM ir_module_module
WHERE name IN ('point_of_sale', 'pos_restaurant', 'pos_self_order');
SELECT id, channel::jsonb AS channel, message::jsonb AS message, create_date
FROM bus_bus WHERE id BETWEEN 87 AND 134 ORDER BY id;
SELECT message::jsonb->>'type' AS type, count(*)
FROM bus_bus WHERE id BETWEEN 87 AND 134 GROUP BY 1;
ROLLBACK;
```

Bus timestamps are transaction timestamps; attribution to a particular RPC comes from the probe's committed-ID mapping rather than timestamp proximity. Recorded method paths independently confirmed 14 `sync_from_ui` operations and two native cancellations. Probe instrumentation calls the original methods once and records real committed row IDs; it does not create these POS publications.

## Why two messages cause two follower reads

The native client subscribes to synchronisation in [DevicesSynchronisation setup](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/static/src/app/utils/devices_synchronisation.js:28). In [the bus notification loop, lines 111–114](/home/coder/worktrees/justpickles00/odoo/master/addons/bus/static/src/services/bus_plugin.js:111), every notification triggers its event. [The subscription wrapper at line 226](/home/coder/worktrees/justpickles00/odoo/master/addons/bus/static/src/services/bus_plugin.js:226) invokes the callback without awaiting it or coalescing equal payloads.

[`collect`, lines 64–86](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/static/src/app/utils/devices_synchronisation.js:64) skips the originating device or a different session via [the helper at lines 247–248](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/static/src/app/utils/devices_synchronisation.js:247). Each remaining notification independently calls `readDataFromServer`; [line 99](/home/coder/worktrees/justpickles00/odoo/master/addons/point_of_sale/static/src/app/utils/devices_synchronisation.js:99) invokes `pos.config.read_config_open_orders`. Equal payloads with distinct bus IDs have no deduplication in this path. An empty static payload still reaches this read.

The browser-free follower trace reproduced that behavior: **32 `read_config_open_orders` RPC starts and completions**, 16 per till, for 16 originating order operations. It also recorded 32 sender skips. Each originating operation's two equal synchronisation rows reached both POS tills: the sender skipped both, and the other till read twice. There were 64 synchronisation deliveries and 32 state-notification deliveries. The analysis reported zero duplicate receipts of the same bus ID, zero unmatched receipts, and no incomplete observed POS-row deliveries.

The first operation gives a concrete concurrent example. Till 1 received rows 87 and 88 in the same websocket frame at monotonic timestamp `330638718651220` ns. Its follow-up operations `posbus_0000:till:1:rpc:5` and `...:rpc:6` started at `330638718746152` and `330638719657962` ns. They completed at `330638750995066` and `330638753655255` ns. The second read started before the first finished. These are two native server RPCs caused by two distinct publications, rather than two log entries for one read.

`ORDER_STATE_CHANGED` is handled separately by the self-order application's [subscription at line 98](/home/coder/worktrees/justpickles00/odoo/master/addons/pos_self_order/static/src/app/services/self_order_service.js:98), which calls `getUserDataFromServer`. The experiment had no self-order application clients and did not emulate that additional follower workload. Regular till follower-read counts above therefore come from `SYNCHRONISATION` alone.

## Scope of the finding

These observations establish redundant publication and read amplification for successful single-order normal POS sync/cancellation with this installed module set, even when self ordering is disabled. For `N` interested POS tills in the same session, the source path implies two invalidation reads per other till, or `2 × (N − 1)` reads per such operation, before cache-related follow-up loads. That is a source-based scaling inference; the concrete SQL/client audit here used `N=2`.

The smoke run demonstrates the publication count and follower call count. It does not by itself establish a server capacity limit, production incident cause, or latency effect at large scale. The emulator has bounded follower concurrency and caches; actual browser behavior was inspected in source rather than executed. No production change is proposed in this document.
