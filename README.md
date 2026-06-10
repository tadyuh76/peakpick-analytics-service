# PeakPick Analytics Service

Owns analytics read models based on consumed domain events.

Owned database tables:

- local `event_log`

Run locally:

```bash
pip install -r requirements.txt
uvicorn services.analytics_service.main:app --reload --port 8007
```
