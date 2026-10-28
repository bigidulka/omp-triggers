#!/usr/bin/env python3
"""Telegram adapter using an existing authorized Telethon session factory.

No credentials are embedded. --config names an owner-only JSON file containing
profile, chat, sessionFactory, webhookUrl, tokenFile, outbox and optionally a
transformer Python file exporting build_event(chat_id, message) -> event|None.
Dependencies (Telethon and session factory) belong to the existing Telegram
runtime, not to the trigger service. See README for lifecycle and replay scope.
"""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import stat
import sys
import time

from omp_outbox import Outbox, event_fingerprint


def load_module(file, name):
    file = Path(file).expanduser().resolve()
    sys.path.insert(0, str(file.parent))
    spec = importlib.util.spec_from_file_location(name, file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def default_event(chat_id, msg):
    return {"eventId": f"telegram:{chat_id}:{msg.id}", "type": "telegram.message", "data": {
        "chatId": chat_id, "messageId": msg.id, "date": msg.date.isoformat() if msg.date else None,
        "text": (msg.message or "")[:4000],
    }}


def capture_message(outbox, event, *, cursor=None, edited=False):
    """Telegram messages can change between live delivery and history catch-up.

    Keep the original source ID for its first immutable version. A changed
    payload gets a deterministic revision ID, never a conflicting retry under
    the original ID. Generic outbox callers retain strict conflict detection.
    """
    if event is not None:
        original_id = event["eventId"]
        fingerprint = event_fingerprint(event)
        known = outbox.known_fingerprint(original_id)
        if (known is not None and known != fingerprint) or (known is None and edited):
            event = dict(event)
            event["eventId"] = f"{original_id[:128]}:revision:{fingerprint[:20]}"
            if isinstance(event.get("data"), dict):
                event["data"] = {**event["data"], "sourceEventId": original_id, "sourceRevision": fingerprint[:20]}
    outbox.enqueue(event, cursor=cursor)


async def collect(client, entity, chat_id, outbox, build_event):
    """Paginate all messages since the durable cursor. Never cap catch-up at 20."""
    cursor = outbox.cursor()
    async for msg in client.iter_messages(entity, min_id=cursor or 0, reverse=True):
        capture_message(outbox, build_event(chat_id, msg), cursor=msg.id)


async def run(config_file, retry_blocked=False):
    info = config_file.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
        raise ValueError("source config must be an owner-only regular file")
    config = json.loads(config_file.read_text())
    factory = load_module(config["sessionFactory"], "omp_telegram_session_factory")
    from telethon import events
    build_event = load_module(config["transformer"], "omp_telegram_transformer").build_event if config.get("transformer") else default_event
    identity = hashlib.sha256(json.dumps({k: config[k] for k in ("profile", "chat", "webhookUrl")}, sort_keys=True).encode()).hexdigest()
    outbox = Outbox(config["outbox"], identity)
    lock = open(str(outbox.file) + ".lock", "a", opener=lambda p, f: os.open(p, f | getattr(os, "O_NOFOLLOW", 0), 0o600))
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if retry_blocked:
        outbox.retry_blocked()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    worker = None
    try:
        async with factory.authorized_client(config["profile"]) as client:
            chat_id = int(config["chat"])
            entity = await client.get_entity(chat_id)
            # The first start begins at the current head. An explicit startAfter
            # is required to process historical messages on initial setup.
            if outbox.cursor() is None:
                head = await client.get_messages(entity, limit=1)
                outbox.enqueue(None, cursor=int(config.get("startAfter", head[0].id if head else 0)))

            async def handler(event):
                try:
                    # Live arrivals never move the contiguous history cursor.
                    capture_message(outbox, build_event(chat_id, event.message), edited=isinstance(event, events.MessageEdited.Event))
                except Exception as exc:
                    print(json.dumps({"sourceError": type(exc).__name__, "messageId": event.message.id}), flush=True)

            client.add_event_handler(handler, events.NewMessage(chats=[chat_id]))
            client.add_event_handler(handler, events.MessageEdited(chats=[chat_id]))

            async def sender():
                while not stop.is_set():
                    await outbox.flush_one(config["webhookUrl"], config["tokenFile"])
                    await asyncio.sleep(0.25)

            worker = asyncio.create_task(sender())
            last_report = 0
            while not stop.is_set():
                # Handler is already attached: history/live overlap is deduped
                # locally, and reconnect gaps are recovered on every scan.
                await collect(client, entity, chat_id, outbox, build_event)
                if time.monotonic() - last_report > 60:
                    print(json.dumps({"ready": True, **outbox.status()}), flush=True)
                    last_report = time.monotonic()
                try:
                    await asyncio.wait_for(stop.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass
    finally:
        stop.set()
        if worker:
            await worker
        outbox.close()
        lock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--retry-blocked", action="store_true", help="after reconciling/fixing an authentication or payload conflict")
    args = parser.parse_args()
    os.umask(0o077)
    asyncio.run(run(args.config, args.retry_blocked))


if __name__ == "__main__":
    main()
