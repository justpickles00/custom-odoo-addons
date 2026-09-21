import { rpc } from "@web/core/network/rpc";

export function subscribe({ stream, snapshotRoute, onCount, onEvent, onState }) {
    let stopped = false;
    let connection;
    let worker;
    let timer;
    let startup;
    let epoch;
    let revision = -1;
    const workerURL = "/sse_server/static/worker/shared_worker.js";

    function accept(message) {
        if (stopped || message.epoch !== epoch) return;
        if (message.payload.count !== undefined && message.seq > revision) {
            revision = message.seq;
            onCount(message.payload.count);
        }
        if (message.payload.operation !== "snapshot") {
            onEvent({ id: `${message.epoch}:${message.seq}`, ...message.payload });
        }
    }

    function receive({ data }) {
        if (stopped) return;
        clearTimeout(startup);
        if (data.kind === "event") {
            accept(data);
        } else if (data.kind === "state") {
            onState(data.state, data.database);
            if (data.state === "live") {
                if (epoch !== data.epoch) {
                    epoch = data.epoch;
                    revision = -1;
                }
                rpc(snapshotRoute).then(accept).catch(() => {
                    if (!stopped) onState("snapshot unavailable");
                });
            }
        }
    }

    function start(info, shared) {
        if (stopped) return;
        worker = shared
            ? new SharedWorker(workerURL, { name: `sse:${info.scope}:${stream}` })
            : new Worker(workerURL);
        connection = shared ? worker.port : worker;
        connection.onmessage = receive;
        connection.start?.();
        const fallback = () => {
            if (stopped || !shared) return;
            clearTimeout(startup);
            connection.postMessage({ kind: "leave" });
            connection.close?.();
            start(info, false);
        };
        worker.onerror = fallback;
        startup = setTimeout(fallback, 4000);
        connection.postMessage({ kind: "start", stream, ...info });
    }

    onState("connecting");
    rpc("/sse/connect", { stream }).then((info) => {
        if (stopped) return;
        try {
            start(info, Boolean(window.SharedWorker));
        } catch {
            start(info, false);
        }
        timer = setInterval(() => connection?.postMessage({ kind: "ping" }), 15000);
    }).catch(() => {
        if (!stopped) onState("access unavailable");
    });

    return () => {
        stopped = true;
        clearInterval(timer);
        clearTimeout(startup);
        connection?.postMessage({ kind: "leave" });
        connection?.close?.();
        worker?.terminate?.();
    };
}
