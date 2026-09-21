import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import time

from odoo.tools import config

MAX_MESSAGE = 262144


def runtime_dir():
    path = Path(config["data_dir"]) / "sse"
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def secret():
    path = runtime_dir() / "key"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return path.read_bytes()
    with os.fdopen(fd, "wb") as file:
        key = os.urandom(32)
        file.write(key)
    return key


def signature(value):
    return hmac.new(secret(), value.encode(), hashlib.sha256).hexdigest()


def ticket(database, uid, session_id, stream):
    claims = {
        "database": database, "uid": uid, "stream": stream,
        "scope": signature(f"{database}:{uid}:{session_id}"),
        "expires": int(time.time()) + 300,
    }
    data = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode()
    return f"{data}.{signature(data)}", claims["scope"]


def verify_ticket(value):
    if len(value) > 4096:
        raise ValueError("Invalid ticket")
    data, signed = value.rsplit(".", 1)
    if not hmac.compare_digest(signature(data), signed):
        raise ValueError("Invalid ticket")
    claims = json.loads(base64.urlsafe_b64decode(data))
    if claims["expires"] <= time.time():
        raise ValueError("Expired ticket")
    return claims


def publish(database, stream, payload):
    """Local delivery acknowledgement, not durable storage."""
    message = json.dumps({
        "database": database, "stream": stream, "payload": payload,
    }, separators=(",", ":")).encode() + b"\n"
    if len(message) > MAX_MESSAGE:
        raise ValueError("SSE message is too large")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(1)
        client.connect(str(runtime_dir() / "publish.sock"))
        client.sendall(message)
        with client.makefile("rb") as reader:
            return json.loads(reader.readline(4096))
