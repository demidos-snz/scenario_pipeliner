"""Broker-owned enums (RFC-0004).

``BrokerOutboxStatus`` lives in ``scenario_pipeliner.db.enums`` so schema DDL
does not import this package. Re-exported here for plugin/broker callers.
"""

from __future__ import annotations

from scenario_pipeliner.db.enums import BrokerOutboxStatus

__all__ = ["BrokerOutboxStatus"]
