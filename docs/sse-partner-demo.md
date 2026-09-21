# First SSE consumer: Live Partner Activity

Status: proposal for discussion; no addon or SSE runtime implemented.
Updated: 2026-09-21. Target: Odoo 20.0 Community.

This proposal refines the user's request for an Owl page that observes
`res.partner` CRUD and displays a live count. It builds on the
[AI harness design](ai-harness-design.md). The choices below are proposed,
not additional decisions already approved by the user.

## 1. Small, visible first feature

Create two addons, provisionally named:

- `sse_server`: generic process integration, IPC, authenticated subscription,
  event routing, SSE delivery, and browser subscription support.
- `sse_partner_demo`: the first consumer; partner instrumentation, stream
  authorization, a count endpoint, and an Owl client-action page in Odoo.

The generic addon must not know about `res.partner`. This first milestone does
not require a model dispatcher, continuation worker, or AI provider.

The page displays:

- Current tenant and connection state: connecting, live, or reconnecting.
- Total partner records, explicitly including archived partners.
- A bounded activity feed, initially the most recent 100 received events.
- Each row's operation, observation time, effective actor user ID, affected
  record IDs/count, and relevant field names. No partner field values.

Count means the current number of partner records, not the number of CRUD
operations. Per-operation counters can be added later if wanted. A partner
record is not necessarily a person: companies and child addresses count too.

Use Owl signals for the feed, total, and connection state. Incoming events
update those signals and the component renders the changes without reloading.

## 2. Precise event semantics

| Operation | What the row means | Delivery timing |
| --- | --- | --- |
| Read | A supported ORM record-value read completed successfully. It does not prove a human viewed the result. | Immediately after that read succeeds. |
| Create | An observed ORM creation survived transaction commit. | After commit. |
| Update | An observed ORM `write` survived transaction commit. The supplied fields may include unchanged values. | After commit. |
| Delete | An observed ORM `unlink` survived transaction commit. | After commit. |

One event can describe a batch of records. One browser action may invoke
several ORM operations, so rows are not equivalent to clicks. Attribute reads,
computed fields, retries, and nested business logic make a universal read audit
substantially larger than this demonstration.

For version one, observe the partner `_read_format` path used by `read`,
`search_read`, and ordinary web form/list reads. Odoo's `search_read` does not
call public `read`, so overriding only `read` would be insufficient. Explicitly
exclude direct cached field access, standalone `search_fetch`/`fetch`, raw SQL,
search/count-only operations, and the web client's ID-only fast path. These
limits must be stated in the demo's help text.

Read events describe observations, not committed business changes. They may
remain visible if an enclosing transaction later rolls back or retries. Avoid
duplicate instrumentation of the same read through multiple overlapping hooks.

Committed mutation events must exclude rolled-back work, including work undone
inside a savepoint. A bare post-commit callback is not sufficient evidence that
savepoint rollback is handled; select and verify a rollback-aware staging
approach during implementation. Capture serializable IDs and field names while
the operation's environment is valid, including IDs before unlink. Never pass
recordsets or cursors to the async worker or dereference deleted records later.

The observer's own authorization, count, and display-related ORM work must not
generate activity recursively. This is a diagnostic feed, not a durable audit
log or a security monitoring feature.

## 3. Count from authoritative snapshots

The demo count endpoint uses `search_count([])` with `active_test=False`.
For the proposed administrator-only, tenant-wide observer, this is deliberately
a whole-tenant count. An explicitly authorized diagnostic endpoint may use
elevated access for that aggregate; ordinary contact operations retain their
normal permissions. Do not accidentally present a record-rule-filtered count
as a tenant-wide total.

The browser subscribes first, then requests its initial count through ordinary
Odoo RPC. Committed mutation events invalidate the count. Coalesce bursts into
a small number of refresh requests; allow one count request in flight and
refresh again if another invalidation arrives during it. Read events do not
refresh the count. Archiving is an update and does not change this total.

Do not increment/decrement the total solely from SSE events. Reconnects, missed
events, and repeated deliveries would cause drift. Reconnection obtains a new
snapshot and marks a break in the activity feed. Show loading/stale status
until the refreshed count arrives. This is eventually refreshed state, not an
atomic snapshot of the feed and database at one global event sequence.

The SSE worker does no SQL and loads no tenant registry. All count queries and
authorization run in ordinary synchronous Odoo requests.

## 4. Audience, actors, and multiple tabs

Start with an administrator-only diagnostic page and stream, scoped to one
database. The subscription endpoint must enforce that permission; hiding the
menu alone is insufficient. It intentionally permits observing partner
operation metadata across users in that database. Do not enable this audience
policy for ordinary users without a separate record-access design.

Routing uses tenant plus stream identity. Each connection has an authenticated,
authorized audience; the event's effective actor user ID is separate. Record
superuser-mode status when available rather than implying that every operation
can be attributed to a human. Background work may use a service user.

A SharedWorker owns the stream connection and distributes events to subscribed
tabs with matching origin, tenant, and authentication scope. A separate browser
or profile has its own connection. Isolate different sessions/users, clean up
subscriptions when tabs leave, and reconnect on authentication changes. Provide
a per-tab fallback when SharedWorker is unavailable.

The same asyncio process serves multiple participating databases. Installing
the demo in another database must not create another SSE process. Queues and
feed history are bounded; a slow client must not block other subscribers or
tenants. Overflow/reset must be visible so the browser can resynchronize.

## 5. Showcase

1. Open Live Partner Activity in one tab and Contacts in another.
2. Create a temporary partner through the normal Contacts UI. The feed shows
   the committed creation and the total refreshes.
3. Open or reload that partner. The feed shows a supported ORM read. Other
   framework-generated partner reads may appear as well.
4. Edit and save a field. The update row identifies the supplied field names;
   the total stays unchanged.
5. Delete the temporary partner. The delete row appears and the total returns
   to its previous value if no other partner changes occurred.
6. Open a second monitor tab. Both update; inspect the SharedWorker connection
   to demonstrate that these same-scope monitor tabs share one SSE stream.
7. Repeat from another logged-in browser to show the actor and audience are
   independent. A second tenant must see only its own activity.

Alongside that visible walkthrough, verify full rollback and savepoint rollback,
multi-record operations, observer recursion suppression, denied subscriptions,
tenant isolation, reconnect/count recovery, and bounded slow-client handling.
The page must be driven by real ORM calls, not canned browser-only events.

## 6. Scope and limits

Live delivery is best effort in this bootstrap. A process can die after a
business commit but before publishing its event. Refreshing the count recovers
current state; it does not reconstruct missed activity. Feed history is bounded
and ephemeral. A durable event log/outbox is a separate requirement if every
event must be recoverable.

This consumer demonstrates ORM-to-IPC-to-SSE-to-Owl delivery, resource-scoped
authorization, and shared browser subscriptions. It does not yet demonstrate
AI orchestration or progress during a long-running model request. Registry
performance optimization remains outside this project's scope.

Source checked: Odoo worktree `20.0`, commit
`efc7cb0f13a1817a068330f14faa8dc811e31f2a`, particularly
`odoo/orm/models.py` (`read`, `_read_format`, `search_read`, `search_count`),
`addons/web/models/models.py` (`web_read`), and `odoo/sql_db.py`
(commit, rollback, and savepoints). Runtime behavior has not yet been tested.
