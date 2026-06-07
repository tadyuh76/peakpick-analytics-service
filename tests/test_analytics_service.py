import pytest
from fastapi.testclient import TestClient

from services.analytics_service.main import app, event_counts, handle_any_event, recent_events
from shared.events import EventType, new_event


client = TestClient(app)


@pytest.mark.asyncio
async def test_operations_summary_counts_member3_lifecycle() -> None:
    event_counts.clear()
    recent_events.clear()

    for event_type in (
        EventType.ORDER_PAID,
        EventType.INVENTORY_RESERVED,
        EventType.PICKUP_SLOT_RESERVED,
        EventType.ORDER_READY,
        EventType.NOTIFICATION_REQUESTED,
        EventType.ORDER_PICKED_UP,
    ):
        await handle_any_event(new_event(event_type, aggregate_id="order-1", source="test", payload={}))

    response = client.get("/operations/summary")

    assert response.status_code == 200
    assert response.json()["orders_ready"] == 1
    assert response.json()["orders_picked_up"] == 1
    assert response.json()["inventory_reservations"] == 1
