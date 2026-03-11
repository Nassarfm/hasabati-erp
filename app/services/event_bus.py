"""
app/services/event_bus.py
══════════════════════════════════════════════════════════
Server-side EventBus.
Decouples modules — posting engine emits events,
inventory/purchases/hr listen independently.

Events emitted:
  je.posted          → accounting posted a JE
  je.reversed        → JE was reversed
  grn.posted         → goods received (inventory updated)
  invoice.posted     → sales/purchase invoice posted
  payroll.posted     → payroll JE created
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Tuple

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class Event:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: str = ""
    payload: dict = field(default_factory=dict)
    tenant_id: str = ""
    source_module: str = "system"
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    processed_by: List[str] = field(default_factory=list)


class EventBus:
    def __init__(self) -> None:
        self._listeners: Dict[str, List[Tuple[Callable, str]]] = {}

    def on(self, event_type: str, *, module: str = "unknown") -> Callable:
        """Decorator to subscribe to an event."""
        def decorator(fn: Callable) -> Callable:
            self._listeners.setdefault(event_type, []).append((fn, module))
            logger.debug("event_subscribed", event=event_type, module=module)
            return fn
        return decorator

    async def emit(
        self,
        event_type: str,
        payload: dict,
        *,
        tenant_id: str = "",
        source_module: str = "system",
    ) -> Event:
        """Emit event — all listeners run concurrently, errors isolated."""
        event = Event(
            type=event_type,
            payload=payload,
            tenant_id=tenant_id,
            source_module=source_module,
        )

        listeners = (
            self._listeners.get(event_type, []) +
            self._listeners.get("*", [])
        )

        if not listeners:
            logger.debug("event_no_listeners", event=event_type)
            return event

        logger.info(
            "event_emit",
            event=event_type,
            listeners=len(listeners),
            source=source_module,
        )

        results = await asyncio.gather(
            *[self._call(fn, event, mod) for fn, mod in listeners],
            return_exceptions=True,
        )

        for (_, mod), result in zip(listeners, results):
            if isinstance(result, Exception):
                logger.error("event_listener_error", module=mod, event=event_type, error=str(result))
            else:
                event.processed_by.append(mod)

        return event

    async def _call(self, fn: Callable, event: Event, module: str) -> None:
        await fn(event)


# Singleton
bus = EventBus()
