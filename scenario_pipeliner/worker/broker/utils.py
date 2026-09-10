"""Broker helper utilities."""

from __future__ import annotations

import hashlib
from typing import Any


def resolve_message_id(
    *,
    queue: str,
    body: bytes,
    amqp_message_id: str | None,
    headers: dict[str, Any] | None = None,
) -> str:
    """Return producer message_id, trace_id, or stable fallback hash.

    Fallback: sha256(queue + NUL + body).
    """
    if isinstance(amqp_message_id, str) and amqp_message_id.strip():
        return amqp_message_id.strip()
    if headers is not None:
        trace_id = str(headers.get("trace_id") or "").strip()
        if trace_id:
            return trace_id
    digest = hashlib.sha256()
    digest.update(queue.encode("utf-8"))
    digest.update(b"\0")
    digest.update(body)
    return digest.hexdigest()
