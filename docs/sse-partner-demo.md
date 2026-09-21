# First SSE consumer: Live Partner Activity

Status: proposal for discussion; no addon or SSE runtime implemented.
Updated: 2026-09-21. Target: Odoo 20.0 Community.

This proposal refines the user's request for an Owl page that observes
`res.partner` create, write, and unlink operations and displays a live count.
Read tracking is explicitly excluded at the user's request. Following the
user's suggestion, create and delete events will carry the count. It builds on
the [AI harness design](ai-harness-design.md). Other choices below remain
proposed unless separately agreed.

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

Count means the current number of partner records, not the number of mutation
operations. Per-operation counters can be added later if wanted. A partner
record is not necessarily a person: companies and child addresses count too.

Use Owl signals for the feed, total, and connection state. Incoming events
update those signals and the component renders the changes without reloading.

## 2. Precise event semantics

| Operation | What the row means | Delivery timing | Count payload |
| --- | --- | --- | --- |
| Create | An observed ORM creation survived transaction commit. | After commit. | Fresh total partner count. |
| Update | An observed ORM `write` survived transaction commit. The supplied fields may include unchanged values. | After commit. | No count needed for the write itself. |
| Delete | An observed ORM `unlink` survived transaction commit. | After commit. | Fresh total partner count. |

One event can describe a batch of records. One browser action may invoke
several ORM operations, so rows are not equivalent to clicks. If a write's
business logic creates or deletes related partners, those create/unlink
operations generate their own events with a count.

Do not instrument `read`, `search_read`, field access, searches, or count
queries. Opening a contact without changing it generates no demo event.
Observe ORM mutations; direct SQL changes are outside this consumer's scope.

Committed mutation events must exclude rolled-back work, including work undone
inside a savepoint. A bare post-commit callback is not sufficient evidence that
savepoint rollback is handled; select and verify a rollback-aware staging
approach during implementation. Capture serializable IDs and field names while
the operation's environment is valid, including IDs before unlink. Never pass
recordsets or cursors to the async worker or dereference deleted records later.

The observer's authorization, count, and display paths must remain read-only
and must not cause partner mutations recursively. This is a diagnostic feed,
not a durable audit log or a security monitoring feature.

## 3. Transmit counts with create and delete events

The demo's count helper uses `search_count([])` with `active_test=False`.
For the proposed administrator-only, tenant-wide observer, this is deliberately
a whole-tenant count. An explicitly authorized diagnostic endpoint may use
elevated access for that aggregate; ordinary contact operations retain their
normal permissions. Do not accidentally present a record-rule-filtered count
as a tenant-wide total.

After a transaction containing partner creates/deletes commits, synchronous
Odoo code obtains a fresh committed-state count in a short, fresh database
transaction and includes it in the outgoing create/delete events. The async
worker forwards the supplied count without querying the database. The browser
updates the activity feed and count signals from the event; it does not make
another count RPC for each mutation.

Several create/delete operations in one committed transaction may reuse one
count snapshot. Every corresponding event carries that snapshot, which describes
the committed total, not an intermediate total after each method call. A batch
create/unlink also needs only one count snapshot, not a query per record.

The count can include other transactions that committed before it was sampled.
It is a snapshot of committed state, not a historical count attributable solely
to the triggering operation. Do not compute it inside the original mutation
transaction and assume it will still be current when the event is delivered.

Count sampling and publication must be ordered per tenant, with an epoch and
revision attached to snapshots. Initial/reconnect snapshots must participate
in the same ordering. The client accepts only newer snapshots within an epoch
and resynchronizes when the epoch changes. A sequence number assigned merely
on arrival cannot correct independently sampled counts that arrived out of
order. The exact synchronous coordination mechanism is an implementation
decision; verify it with concurrent workers before claiming correct ordering.

The browser subscribes first, then obtains its initial count through an
ordinary Odoo RPC using that snapshot mechanism. Ignore an initial RPC result
if a newer event snapshot has already arrived. Reconnection or an explicit
stream reset obtains a fresh snapshot and marks a break in the activity feed.
Show loading/stale status until the refreshed count arrives. Archiving is an
update and does not change this total.

Replace the displayed total with the received absolute count; do not increment
or decrement it based on event type. A later snapshot can correct drift from a
missed event, but cannot reconstruct missing activity. Between snapshots the
display can lag committed state; this is not an atomic view of the feed and
database at one global business-transaction sequence.

The SSE worker does no SQL and loads no tenant registry. Count queries run in
the synchronous Odoo publisher or snapshot RPC; authorization also remains
on the synchronous side. No second PostgreSQL driver is required.

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
   the committed creation and its event supplies the new total.
3. Edit and save a field. The update row identifies the supplied field names;
   the total stays unchanged.
4. Delete the temporary partner. Its event supplies the refreshed total, which
   returns to its previous value if no other partner changes occurred.
5. Open a second monitor tab. Both update; inspect the SharedWorker connection
   to demonstrate that these same-scope monitor tabs share one SSE stream.
6. Repeat from another logged-in browser to show the actor and audience are
   independent. A second tenant must see only its own activity.

Alongside that visible walkthrough, verify full rollback and savepoint rollback,
multi-record operations, concurrent create/delete snapshot ordering, delayed
initial RPC results, no events on reads, denied subscriptions, tenant isolation,
reconnect/count recovery, and bounded slow-client handling.
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
`odoo/orm/models.py` (`create`, `write`, `unlink`, `search_count`) and
`odoo/sql_db.py` (commit, rollback, and savepoints). Runtime behavior has not
yet been tested.
