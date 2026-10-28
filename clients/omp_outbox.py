"""Durable source outbox. A source cursor means captured locally, not delivered.

Use enqueue(event, cursor=...) in the same transaction as source progress. Only
HTTP 202 with a receipt removes a pending event. 409 is never called a duplicate.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import stat
import time
import urllib.error
from pathlib import Path

from omp_events import emit


class Outbox:
    def __init__(self, file, source_key):
        self.file = Path(file)
        self.file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = self.file.parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
            raise ValueError("outbox directory must be private")
        fd = os.open(self.file, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        info = os.fstat(fd)
        os.close(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
            raise ValueError("unsafe outbox file")
        self.db = sqlite3.connect(self.file)
        self.db.executescript("""PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,body TEXT NOT NULL,fingerprint TEXT NOT NULL,
            state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,
            error TEXT,receipt TEXT,created REAL NOT NULL);""")
        with self.db:
            previous = self.db.execute("SELECT value FROM metadata WHERE key='source'").fetchone()
            if previous and previous[0] != source_key:
                raise ValueError("outbox belongs to another source/target")
            self.db.execute("INSERT OR IGNORE INTO metadata VALUES('source',?)", (source_key,))

    def close(self):
        self.db.close()

    def cursor(self):
        row = self.db.execute("SELECT value FROM metadata WHERE key='cursor'").fetchone()
        return int(row[0]) if row else None

    def enqueue(self, event, *, cursor=None):
        """Filtering may pass event=None while advancing a scanned source cursor."""
        with self.db:
            if event is not None:
                body = json.dumps(event, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
                if len(body.encode()) > 16384:
                    raise ValueError("event exceeds receiver limit")
                fingerprint = hashlib.sha256(body.encode()).hexdigest()
                prior = self.db.execute("SELECT fingerprint FROM events WHERE id=?", (event["eventId"],)).fetchone()
                if prior and prior[0] != fingerprint:
                    raise ValueError("source event changed under the same ID")
                if not prior:
                    pending = self.db.execute("SELECT count(*) FROM events WHERE state!='accepted'").fetchone()[0]
                    if pending >= 10000:
                        raise ValueError("outbox full; source cursor was not advanced")
                    self.db.execute("INSERT INTO events(id,body,fingerprint,state,created) VALUES(?,?,?,'pending',?)", (event["eventId"], body, fingerprint, time.time()))
            if cursor is not None:
                self.db.execute("INSERT INTO metadata VALUES('cursor',?) ON CONFLICT(key) DO UPDATE SET value=cast(max(cast(value AS INTEGER),cast(excluded.value AS INTEGER)) AS TEXT)", (str(cursor),))

    def next(self):
        row = self.db.execute("SELECT id,body,attempts,retry_at,state FROM events WHERE state!='accepted' ORDER BY rowid LIMIT 1").fetchone()
        if not row or row[4] == "blocked" or row[3] > time.time():
            return None
        return {"id": row[0], "event": json.loads(row[1]), "attempts": row[2]}

    def status(self):
        return {"cursor": self.cursor(), "counts": dict(self.db.execute("SELECT state,count(*) FROM events GROUP BY state"))}

    def retry_blocked(self):
        with self.db:
            self.db.execute("UPDATE events SET state='pending',retry_at=0 WHERE state='blocked'")

    async def flush_one(self, url, token_file):
        item = self.next()
        if item is None:
            return False
        event = item["event"]
        try:
            receipt = await asyncio.to_thread(emit, url, token_file, event_id=event["eventId"], event_type=event["type"], data=event["data"])
            if not receipt.get("receiptId") or not receipt.get("state"):
                raise ValueError("receiver returned no durable receipt")
            with self.db:
                self.db.execute("UPDATE events SET state='accepted',receipt=?,error=NULL WHERE id=?", (json.dumps(receipt), item["id"]))
                # Bounded accepted detail; receiver has its own dedup ledger.
                self.db.execute("DELETE FROM events WHERE state='accepted' AND created<?", (time.time() - 7 * 86400,))
            return True
        except urllib.error.HTTPError as exc:
            try:
                code = json.loads(exc.read(2048)).get("error", "http_error")
            except (ValueError, AttributeError):
                code = "http_error"
            retryable = exc.code >= 500 or exc.code == 429 or (exc.code == 409 and code == "subscription_paused")
            self._failed(item, f"HTTP {exc.code}: {code}", blocked=not retryable)
        except (OSError, TimeoutError) as exc:
            self._failed(item, type(exc).__name__)
        except ValueError as exc:
            self._failed(item, str(exc), blocked=True)
        return False

    def _failed(self, item, code, blocked=False):
        delay = min(60, 2 ** min(item["attempts"], 6))
        with self.db:
            self.db.execute("UPDATE events SET state=?,attempts=attempts+1,retry_at=?,error=? WHERE id=?", ("blocked" if blocked else "pending", time.time() + delay, code, item["id"]))
