"""Broker ingress/egress exceptions (RFC-0004)."""

from __future__ import annotations


class IngressReject(Exception):
    """Non-retryable validation error → nack/DLQ (no tasks / broker row)."""


class IngressRetry(Exception):
    """Transient error → nack/requeue or delayed retry policy."""


class DuplicateBrokerMessage(Exception):
    """Raised when (ingress_queue, message_id) already exists."""

    def __init__(self, *, task_id: int | None = None) -> None:
        self.task_id = task_id
        super().__init__("duplicate broker ingress message")
