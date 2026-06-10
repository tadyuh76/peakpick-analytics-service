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


def test_operations_summary_uses_persisted_counts_when_database_is_enabled(monkeypatch) -> None:
    async def fake_analytics_from_event_log(store_id: str) -> dict[str, object]:
        assert store_id == "store-ueh"
        return {
            "counts": {
                EventType.ORDER_PAID.value: 4,
                EventType.ORDER_READY.value: 3,
                EventType.ORDER_PICKED_UP.value: 2,
                EventType.INVENTORY_RESERVED.value: 4,
                EventType.NOTIFICATION_REQUESTED.value: 3,
            },
            "recent_events": [],
        }

    monkeypatch.setattr("services.analytics_service.main._database_enabled", lambda: True)
    monkeypatch.setattr("services.analytics_service.main._analytics_from_event_log", fake_analytics_from_event_log)

    response = client.get("/operations/summary", headers={"x-store-id": "store-ueh"})

    assert response.status_code == 200
    assert response.json() == {
        "orders_paid": 4,
        "orders_ready": 3,
        "orders_picked_up": 2,
        "inventory_reservations": 4,
        "inventory_shortages": 0,
        "notifications_requested": 3,
        "pickup_completion_rate": 0.5,
    }
