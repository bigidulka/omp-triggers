"""Deliver a source event to an OMP subscription without exposing its credential.

Usage from an authorized Telegram/wallet listener:
    emit(webhook_url, token_file, event_id="telegram:channel:message", event_type="telegram.alert", data={"text": text})
"""
import json
import os
import stat
import ipaddress
from urllib.parse import urlsplit
import urllib.request


def emit(webhook_url, token_file, *, event_id, event_type, data, timeout=10):
    """HTTP 202 acknowledges durable intake, not completion of the agent turn.

    For a network timeout the source may resend IDENTICAL data with the SAME
    event_id. The receiver deduplicates it. Never generate a new ID on retry.
    """
    url = urlsplit(webhook_url)
    loopback = url.hostname == "localhost"
    try:
        loopback = loopback or ipaddress.ip_address(url.hostname).is_loopback
    except ValueError:
        pass
    if url.username or url.password or url.scheme not in ("http", "https") or (url.scheme == "http" and not loopback):
        raise ValueError("use HTTPS or loopback HTTP for a credentialed webhook")
    fd = os.open(token_file, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "r", encoding="utf-8") as credential:
        info = os.fstat(credential.fileno())
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077 or info.st_uid != os.getuid():
            raise ValueError("subscription credential must be an owner-only regular file")
        token = credential.read(128).strip()
    body = json.dumps({"eventId": event_id, "type": event_type, "data": data}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(webhook_url, body, {"Authorization": "Bearer " + token, "Content-Type": "application/json"}, method="POST")
    # Do not forward Bearer credentials through redirects.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    with urllib.request.build_opener(NoRedirect).open(request, timeout=timeout) as response:
        if response.status != 202:
            raise ValueError("receiver did not acknowledge durable acceptance (HTTP 202)")
        return json.load(response)
