# Realtime Stream Crypto

Pipeline phân tích giao dịch crypto thời gian thực theo mô hình medallion, lấy dữ liệu `aggTrade` từ Binance và cung cấp dashboard/alert qua Grafana.

```text
Binance WebSocket → Python producer → Kafka → Bronze Delta (MinIO)
                                      └→ Silver Delta (1-minute aggregates)
                                           └→ Gold analytics + alerts (ClickHouse) → Grafana
```

## Thành phần

| Thành phần | Vai trò | Cổng local |
| --- | --- | --- |
| Kafka / Kafka UI | Event bus / xem topic | `9092` / `8082` |
| MinIO | Lakehouse object store | `9000` / `9001` |
| Spark Master / Worker | Structured Streaming | `8080` / `8081` |
| ClickHouse | Gold serving layer | `8123` |
| Grafana | Dashboard và unified alerting | `3000` |

## Chuẩn bị

Tạo `.env` (không commit) với tối thiểu:

```dotenv
MINIO_USER=replace-me
MINIO_PASS=replace-me
MINIO_ENDPOINT=http://minio:9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASS=replace-me
GRAFANA_PASS=replace-me
KAFKA_HOST=localhost
KAFKA_INTERNAL_HOST=kafka:29092
SPARK_MASTER_URL=spark://spark-master:7077

# Tùy chọn MVP
CRYPTO_SYMBOLS=btcusdt,ethusdt
ALERT_PRICE_CHANGE_PCT=1.0
SILVER_WATERMARK=5 minutes
```

Khởi tạo hạ tầng và schema ClickHouse:

```bash
docker compose up -d
docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASS" < scripts/init_clickhouse.sql
```

> Nạp biến `.env` vào shell trước khi chạy câu lệnh schema, ví dụ: `set -a; source .env; set +a` trên Bash.

## Chạy pipeline local

Cài dependency producer rồi chạy producer trong terminal riêng:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python apps/producer.py
```

Chạy ba Spark job theo thứ tự. Các script PowerShell đã ghim package cần thiết:

```powershell
./scripts/run_streaming.ps1
./scripts/run_silver_aggregation.ps1
./scripts/run_gold_analytics.ps1
```

Mỗi script chạy blocking; hãy mở terminal riêng cho từng job. CI/CD tại [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) tự khởi động các job theo cùng thứ tự.

## Contract dữ liệu và chất lượng

- **Bronze** [`apps/spark_streaming.py`](apps/spark_streaming.py): parse JSON, ép `price`/`quantity` sang `double`, bắt buộc `event_id`, `symbol`, event time và giá/khối lượng dương. Bản ghi lỗi được ghi Delta tại `s3a://crypto-lake/quarantine/crypto_trades`.
- **Silver** [`apps/spark_silver_aggregation.py`](apps/spark_silver_aggregation.py): đọc Bronze Delta, watermark event-time mặc định 5 phút, loại event ID trùng trong trạng thái stream, phát aggregate VWAP theo một phút sau khi cửa sổ đóng.
- **Gold** [`apps/spark_gold_analytics.py`](apps/spark_gold_analytics.py): đọc Silver Delta, tính phần trăm chênh lệch VWAP từ cửa sổ trước của từng symbol, và ghi bảng ClickHouse dạng `ReplacingMergeTree`.

## Dashboard và cảnh báo

Grafana tự provision datasource, dashboard **Crypto Realtime Overview** và alert rule sau khi container restart.

- Dashboard: `http://localhost:3000`, folder **Crypto Streaming**.
- Panel: VWAP, biến động phần trăm, volume, trade count và bảng lịch sử alert.
- Alert rule: kích hoạt nếu biến động tuyệt đối lớn nhất trong năm phút gần nhất lớn hơn **1%**. Điều chỉnh ngưỡng phát event alert trong Gold bằng `ALERT_PRICE_CHANGE_PCT`; nếu thay đổi ngưỡng này, cập nhật tương ứng rule tại [`grafana/provisioning/alerting/crypto-price-volatility.yml`](grafana/provisioning/alerting/crypto-price-volatility.yml).

## Xác minh nhanh

```bash
docker exec clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASS" --query "SELECT symbol, window_start, vwap, price_change_pct FROM default.gold_crypto_analytics_v2 FINAL ORDER BY window_start DESC LIMIT 20"

docker exec clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASS" --query "SELECT symbol, direction, price_change_pct, created_at FROM default.crypto_price_alerts FINAL ORDER BY created_at DESC LIMIT 20"
```

## Chuyển đổi từ pipeline cũ / reset state

MVP dùng contract Delta Bronze → Silver → Gold mới. Khi chuyển từ phiên bản cũ hoặc thay đổi schema/checkpoint không tương thích:

1. Dừng toàn bộ Spark jobs.
2. Sao lưu dữ liệu/checkpoint MinIO nếu cần audit.
3. Xóa checkpoint của tầng cần chạy lại dưới `s3a://crypto-lake/checkpoints/`.
4. Nếu muốn reprocess đầy đủ, xóa output Delta phụ thuộc và truncate hai bảng Gold v2/alerts.
5. Chạy lại Bronze, Silver, Gold theo thứ tự.

Không xóa checkpoint của pipeline đang chạy bình thường: checkpoint là cơ chế khôi phục offset và xử lý lại an toàn.
