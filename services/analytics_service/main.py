from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from shared.event_bus import build_event_bus
from shared.events import EventEnvelope, EventType, new_event
from shared.logging import configure_logging, install_api_logging, log_event
from shared.settings import get_settings
from shared.tenancy import DEFAULT_STORE_ID, store_id_from_event_payload, store_id_from_request


settings = get_settings("analytics-service")
logger = configure_logging(settings.service_name)
event_counts: Counter[str] = Counter()
recent_events: list[dict[str, object]] = []


def _database_enabled() -> bool:
    return bool(settings.database_url)


async def _analytics_from_event_log(store_id: str = DEFAULT_STORE_ID) -> dict[str, object]:
    return await asyncio.to_thread(_analytics_from_event_log_sync, store_id)


def _analytics_from_event_log_sync(store_id: str) -> dict[str, object]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        count_rows = conn.execute(
            """
            SELECT event_type, COUNT(*) AS count
            FROM event_log
            WHERE store_id = %s
            GROUP BY event_type
            """
            ,
            (store_id,),
        ).fetchall()
        recent_rows = conn.execute(
            """
            SELECT event_type, aggregate_id, correlation_id, source, store_id
            FROM event_log
            WHERE store_id = %s
            ORDER BY occurred_at DESC, created_at DESC
            LIMIT 20
            """,
            (store_id,),
        ).fetchall()

    return {
        "counts": {str(row["event_type"]): int(row["count"]) for row in count_rows},
        "recent_events": [dict(row) for row in reversed(recent_rows)],
    }


async def handle_any_event(event: EventEnvelope) -> None:
    event_counts[str(event.event_type)] += 1
    recent_events.append(
        {
            "event_type": event.event_type,
            "aggregate_id": event.aggregate_id,
            "correlation_id": event.correlation_id,
            "source": event.source,
            "store_id": store_id_from_event_payload(event.payload),
        }
    )
    del recent_events[:-20]


def operations_summary() -> dict[str, object]:
    paid = event_counts.get(EventType.ORDER_PAID.value, 0)
    picked_up = event_counts.get(EventType.ORDER_PICKED_UP.value, 0)
    return {
        "orders_paid": paid,
        "orders_ready": event_counts.get(EventType.ORDER_READY.value, 0),
        "orders_picked_up": picked_up,
        "inventory_reservations": event_counts.get(EventType.INVENTORY_RESERVED.value, 0),
        "inventory_shortages": event_counts.get(EventType.INVENTORY_SHORTAGE_DETECTED.value, 0),
        "notifications_requested": event_counts.get(EventType.NOTIFICATION_REQUESTED.value, 0),
        "pickup_completion_rate": picked_up / paid if paid else 0,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    event_bus = build_event_bus(settings)
    await event_bus.connect()
    await event_bus.subscribe("*", handle_any_event, queue_name=f"{settings.service_name}.all-events")
    app.state.event_bus = event_bus
    log_event(logger, settings.service_name, "event subscriptions ready", bus=settings.event_bus)
    try:
        yield
    finally:
        await event_bus.close()


app = FastAPI(
    title="PeakPick Analytics Service",
    version="0.1.0",
    description="Event counters, slot utilization, and peak-hour demand signals.",
    lifespan=lifespan,
)
install_api_logging(app, logger, settings.service_name)


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    return {
        "status": "ok",
        "service": settings.service_name,
        "event_bus_connected": request.app.state.event_bus.is_connected,
    }


@app.get("/events")
async def get_event_counts(request: Request) -> dict[str, object]:
    if _database_enabled():
        return await _analytics_from_event_log(store_id_from_request(request))
    return {"counts": dict(event_counts), "recent_events": recent_events}


@app.get("/operations/summary")
async def get_operations_summary() -> dict[str, object]:
    return operations_summary()


@app.post("/snapshot")
async def publish_snapshot(request: Request) -> dict[str, object]:
    payload = {"counts": dict(event_counts), "recent_events": recent_events[-5:]}
    event = new_event(
        EventType.ANALYTICS_UPDATED,
        aggregate_id="analytics",
        source=settings.service_name,
        payload=payload,
    )
    await request.app.state.event_bus.publish(event)
    log_event(logger, settings.service_name, "analytics snapshot published")
    return payload
