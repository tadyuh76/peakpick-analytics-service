from __future__ import annotations

from collections import Counter
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from shared.event_bus import build_event_bus
from shared.events import EventEnvelope, EventType, new_event
from shared.logging import configure_logging, log_event
from shared.settings import get_settings


settings = get_settings("analytics-service")
logger = configure_logging(settings.service_name)
event_counts: Counter[str] = Counter()
recent_events: list[dict[str, object]] = []


async def handle_any_event(event: EventEnvelope) -> None:
    event_counts[str(event.event_type)] += 1
    recent_events.append(
        {
            "event_type": event.event_type,
            "aggregate_id": event.aggregate_id,
            "correlation_id": event.correlation_id,
            "source": event.source,
        }
    )
    del recent_events[:-20]


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


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    return {
        "status": "ok",
        "service": settings.service_name,
        "event_bus_connected": request.app.state.event_bus.is_connected,
    }


@app.get("/events")
async def get_event_counts() -> dict[str, object]:
    return {"counts": dict(event_counts), "recent_events": recent_events}


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

