# SSE server bootstrap — Odoo master

Pair this branch with Odoo's `master` worktree (currently 20.1 development).

`sse_server` adds one asyncio process to Odoo's Linux prefork server.
`sse_contacts_demo` provides the administrator-only **Live Contacts Activity**
page at `/odoo/live-contacts`, tracking committed partner create/write/unlink.

- Install `aiohttp` in Odoo's Python environment.
- Include this directory in `addons_path`, use `workers > 0`, and load
  `--load=base,web,sse_server`. Install `sse_contacts_demo` in participating DBs.
- Proxy `/sse/stream` to `127.0.0.1:8073` without buffering, with a streaming
  timeout above 30 seconds. Set `ODOO_SSE_PORT` to change that port. Other SSE
  routes go to normal Odoo HTTP workers. Preserve the same browser origin.
- Use a separate Odoo data directory per deployment. The local publishing
  socket, signing key, and count locks live in its private `sse` directory.

Create/delete events carry the absolute count, including archived partners.
SharedWorker shares streams between tabs; dedicated Worker is the fallback.
Tickets expire after five minutes and are renewed through Odoo authorization.
Delivery is best effort, with bounded queues and no replay. Reconnect refreshes
the count. Internal transient rows stage mutations through commit/savepoints;
they are not a durable delivery queue. There are no read hooks or automated
tests in this bootstrap.
