# PeakPick Analytics Service

Analytics Service là microservice đọc domain events và tạo thống kê vận hành đơn giản.

## Database Riêng

Service này sở hữu database `peakpick_analytics` với bảng:

- `event_log`

## Trách Nhiệm

- Đếm số lượng event quan trọng.
- Trả snapshot gần nhất cho dashboard.
- Tính summary như số đơn đã thanh toán, sẵn sàng và đã pickup.

## Chạy Local

```bash
pip install -r requirements.txt
uvicorn services.analytics_service.main:app --reload --port 8007
```
