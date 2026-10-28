#!/usr/bin/env python3
"""Standard-library client for omp-session-bridge Unix sockets."""
from __future__ import annotations
import argparse
import json
import os
import pathlib
import socket
import stat
import sys
import tempfile
import time
import uuid

PROTOCOL_VERSION = 1

def bridge_dir() -> pathlib.Path:
    return pathlib.Path(os.environ.get("OMP_SESSION_BRIDGE_DIR") or os.environ.get("XDG_RUNTIME_DIR", "") and os.path.join(os.environ["XDG_RUNTIME_DIR"], "omp-session-bridge") or os.path.join(tempfile.gettempdir(), f"omp-session-bridge-{os.getuid()}"))

def _secure_record(path: pathlib.Path) -> dict:
    file_stat = path.lstat()
    directory_stat = path.parent.lstat()
    uid = os.getuid()
    if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode) or stat.S_ISLNK(directory_stat.st_mode) or not stat.S_ISDIR(directory_stat.st_mode) or file_stat.st_uid != uid or directory_stat.st_uid != uid:
        raise RuntimeError(f"unsafe registry: {path}")
    if directory_stat.st_mode & 0o077 or file_stat.st_mode & 0o077:
        raise RuntimeError(f"registry is not private: {path}")
    record = json.loads(path.read_text())
    if record.get("protocolVersion") != PROTOCOL_VERSION or not isinstance(record.get("socketPath"), str):
        raise RuntimeError(f"invalid registry: {path}")
    return record

def list_sessions(directory: str | os.PathLike | None = None) -> list[dict]:
    root = pathlib.Path(directory) if directory else bridge_dir()
    if not root.exists(): return []
    records = []
    for candidate in root.glob("*.json"):
        try: records.append(_secure_record(candidate))
        except (OSError, ValueError, RuntimeError): pass
    return sorted(records, key=lambda item: item.get("startedAt", ""), reverse=True)

def _target(session_id=None, session_file=None, name=None, socket_path=None, directory=None) -> dict:
    if socket_path: return {"socketPath": socket_path}
    selectors = (("sessionId", session_id), ("sessionFile", session_file), ("name", name))
    field, target = next(((field, value) for field, value in selectors if value is not None), (None, None))
    matches = [r for r in list_sessions(directory) if (field is None and (target is None or r.get("sessionId") == target or r.get("sessionFile") == target or r.get("name") == target or r.get("sessionId", "").startswith(target))) or (field is not None and r.get(field) == target)]
    if not matches: raise RuntimeError(f"no live OMP bridge matches {target!r}" if target else "no live OMP session bridges found")
    if len(matches) != 1: raise RuntimeError("target is ambiguous; specify full session_id or socket_path")
    return matches[0]

def _request(record: dict, method: str, prompt: str | None = None, params: dict | None = None, timeout: float = 300) -> dict:
    request = {"id": str(uuid.uuid4()), "method": method}
    if prompt is not None:
        if not prompt.strip(): raise ValueError("prompt must not be empty")
        if len(prompt.encode()) > 32768: raise ValueError("prompt exceeds 32768 bytes")
        request["prompt"] = prompt
    if params is not None: request["params"] = params
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(record["socketPath"])
        client.sendall((json.dumps(request, separators=(",", ":")) + "\n").encode())
        file = client.makefile("rb")
        while True:
            raw = file.readline(65537)
            if not raw: raise RuntimeError("bridge closed before final response")
            if len(raw) > 65536 or not raw.endswith(b"\n"): raise RuntimeError("invalid bridge response framing")
            response = json.loads(raw)
            if response.get("type") == "error":
                error = response.get("error", {})
                raise RuntimeError(f"{error.get('code', 'error')}: {error.get('message', 'bridge error')}")
            if response.get("type") == "result": return response["result"]

def call(*, session_id=None, prompt: str, session_file=None, name=None, socket_path=None, timeout=300, directory=None) -> str:
    return _request(_target(session_id, session_file, name, socket_path, directory), "prompt", prompt, timeout=timeout)["text"]

def ping(*, session_id=None, session_file=None, name=None, socket_path=None, timeout=300, directory=None) -> dict:
    return _request(_target(session_id, session_file, name, socket_path, directory), "ping", timeout=timeout)

def status(*, session_id=None, session_file=None, name=None, socket_path=None, timeout=300, directory=None) -> dict:
    return _request(_target(session_id, session_file, name, socket_path, directory), "status", timeout=timeout)

def _schedule(operation: str, *, session_id=None, session_file=None, name=None, socket_path=None, timeout=300, directory=None, **params) -> dict:
    return _request(_target(session_id, session_file, name, socket_path, directory), f"schedule.{operation}", params={k: v for k, v in params.items() if v is not None}, timeout=timeout)

def schedule_create(*, prompt: str, at=None, every=None, cron=None, expires=None, jitter=None, max_queue_wait=None, coalesce_key=None, idempotency_key=None, **target) -> dict:
    return _schedule("create", prompt=prompt, at=at, every=every, cron=cron, expires=expires, jitter=jitter, maxQueueWait=max_queue_wait, coalesceKey=coalesce_key, idempotencyKey=idempotency_key, **target)
def schedule_list(**target) -> list[dict]: return _schedule("list", **target)["tasks"]
def schedule_get(task_id: str, **target) -> dict: return _schedule("get", taskId=task_id, **target)["task"]
def schedule_update(task_id: str, **changes) -> dict: return _schedule("update", taskId=task_id, **changes)
def schedule_delete(task_id: str, **target) -> dict: return _schedule("delete", taskId=task_id, **target)
def schedule_pause(task_id: str, **target) -> dict: return _schedule("pause", taskId=task_id, **target)
def schedule_resume(task_id: str, **target) -> dict: return _schedule("resume", taskId=task_id, **target)
def schedule_run_now(task_id: str, *, idempotency_key=None, **target) -> dict: return _schedule("run_now", taskId=task_id, idempotencyKey=idempotency_key, **target)
def schedule_history(task_id: str | None = None, *, limit=20, **target) -> list[dict]: return _schedule("history", taskId=task_id, limit=limit, **target)["history"]

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs="?", default="prompt", choices=("list", "ping", "status", "prompt", "schedule"))
    parser.add_argument("prompt", nargs="?")
    parser.add_argument("--session-id"); parser.add_argument("--session-file"); parser.add_argument("--name"); parser.add_argument("--socket", dest="socket_path")
    parser.add_argument("--timeout", type=float, default=300); parser.add_argument("--directory"); parser.add_argument("--json", action="store_true"); parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--action", choices=("list", "add", "show", "update", "delete", "pause", "resume", "run-now", "history")); parser.add_argument("--task-id")
    parser.add_argument("--at"); parser.add_argument("--every"); parser.add_argument("--cron"); parser.add_argument("--expires"); parser.add_argument("--jitter"); parser.add_argument("--idempotency-key"); parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(); target = {k: getattr(args, k) for k in ("session_id", "session_file", "name", "socket_path", "timeout", "directory")}
    try:
        if args.command == "list": result = list_sessions(args.directory)
        elif args.command == "schedule":
            action = args.action or "list"
            if action == "add":
                prompt = sys.stdin.read() if args.stdin else args.prompt
                if not prompt: parser.error("schedule add requires prompt or --stdin")
                result = schedule_create(prompt=prompt, at=args.at, every=args.every, cron=args.cron, expires=args.expires, jitter=args.jitter, idempotency_key=args.idempotency_key, **target)
            elif action == "list": result = schedule_list(**target)
            elif action == "show": result = schedule_get(args.task_id, **target)
            elif action == "update": result = schedule_update(args.task_id, prompt=args.prompt, expires=args.expires, jitter=args.jitter, **target)
            elif action == "delete": result = schedule_delete(args.task_id, **target)
            elif action == "pause": result = schedule_pause(args.task_id, **target)
            elif action == "resume": result = schedule_resume(args.task_id, **target)
            elif action == "run-now": result = schedule_run_now(args.task_id, idempotency_key=args.idempotency_key, **target)
            else: result = schedule_history(args.task_id, limit=args.limit, **target)
        elif args.command == "prompt":
            prompt = sys.stdin.read() if args.stdin else args.prompt
            if prompt is None: parser.error("prompt required (or --stdin)")
            result = call(prompt=prompt, **target)
        else: result = globals()[args.command](**target)
        print(json.dumps(result) if args.json or not isinstance(result, str) else result)
        return 0
    except Exception as error:
        print(f"omp_session_bridge: {error}", file=sys.stderr); return 1
if __name__ == "__main__": raise SystemExit(main())
