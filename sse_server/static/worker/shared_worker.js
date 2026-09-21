// Standalone worker: deliberately excluded from Odoo's page asset bundle.
const clients = new Map();
let identity;
let source;
let retry;
let state = { kind: "state", state: "connecting" };

function broadcast(message) {
    for (const port of clients.keys()) port.postMessage(message);
}

function status(value, epoch) {
    state = { kind: "state", state: value, epoch, database: identity.database };
    broadcast(state);
}

function connect(url) {
    if (!clients.size) return;
    source = new EventSource(url);
    source.onmessage = ({ data }) => {
        const message = JSON.parse(data);
        if (message.kind === "ready") status("live", message.epoch);
        else broadcast(message);
    };
    source.onerror = () => {
        source.close();
        status("reconnecting");
        clearTimeout(retry);
        retry = setTimeout(renew, 1500);
    };
}

async function renew() {
    if (!clients.size) return;
    try {
        const response = await fetch("/sse/connect", {
            method: "POST", credentials: "same-origin",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ jsonrpc: "2.0", method: "call", id: 1,
                params: { stream: identity.stream } }),
        });
        const { result, error } = await response.json();
        if (error || !result || result.scope !== identity.scope) {
            status("access expired — reload page");
            return;
        }
        connect(result.url);
    } catch {
        status("reconnecting");
        retry = setTimeout(renew, 3000);
    }
}

function leave(port) {
    clients.delete(port);
    port.close?.();
    if (!clients.size) {
        source?.close();
        clearTimeout(retry);
        self.close();
    }
}

function attach(port) {
    port.onmessage = ({ data }) => {
        if (data.kind === "start") {
            if (identity && (identity.scope !== data.scope || identity.stream !== data.stream)) {
                port.postMessage({ kind: "state", state: "access expired — reload page" });
                return;
            }
            clients.set(port, Date.now());
            if (!identity) {
                identity = data;
                state.database = data.database;
                connect(data.url);
            }
            port.postMessage(state);
        } else if (data.kind === "ping" && clients.has(port)) {
            clients.set(port, Date.now());
        } else if (data.kind === "leave") {
            leave(port);
        }
    };
    port.start?.();
}

setInterval(() => {
    for (const [port, seen] of clients) {
        if (Date.now() - seen > 90000) leave(port);
    }
}, 30000);

if ("onconnect" in self) self.onconnect = ({ ports }) => attach(ports[0]);
else attach(self);
