# P0 Runbook: tính đúng đắn dữ liệu, replay và đối soát

Tài liệu này áp dụng cho pipeline Binance → Kafka → Bronze Delta → Silver Delta → ClickHouse Gold.

## Mục tiêu và bảo đảm hiện có

- Mỗi aggregate trade mới từ Binance có định danh ổn định: `binance:aggTrade:<symbol>:<aggregate_trade_id>`.
- Bronze dùng `event_id` làm merge key. Việc producer reconnect hoặc Kafka replay không được tạo thêm giao dịch logic tại Bronze.
- Silver loại bản ghi trùng theo `event_id` trong trạng thái streaming, sau đó tạo aggregate theo cặp `(symbol, window_start)`.
- Gold sử dụng khóa tự nhiên `(symbol, window_start)` trong ClickHouse `ReplacingMergeTree`; alert dùng `alert_id` tất định.
- Gold tính `price_change_pct` tuần tự theo `window_start` của từng symbol, kể cả khi một micro-batch có nhiều cửa sổ.

Các bảo đảm trên là at-least-once ở tầng vận chuyển và idempotent theo khóa nghiệp vụ tại các sink. Việc ghi analytics và alerts là hai thao tác JDBC riêng; trước khi nâng cấp lên transactional outbox, cần đối soát cả hai bảng sau một lần recovery.

## Không được thực hiện trong production

- Không xóa checkpoint của query đang chạy bình thường.
- Không chạy một job replay cùng checkpoint hoặc cùng output path với job production.
- Không chạy `VACUUM` trước khi replay window và yêu cầu audit đã hết hạn.
- Không chạy backfill thẳng vào bảng Gold production trước khi đối soát kết quả ở namespace tách biệt.

## Quy trình incident và restart thông thường

1. Ghi nhận UTC start/end của khoảng dữ liệu bị ảnh hưởng, Kafka topic/partition/offset nếu biết và phiên bản image/job đang chạy.
2. Dừng đúng job lỗi; giữ nguyên checkpoint và Delta log.
3. Kiểm tra Kafka lag, Spark query progress, watermark, Quarantine rate và lỗi ClickHouse.
4. Khởi động lại cùng phiên bản job, input/output path và checkpoint path. Structured Streaming sẽ tiếp tục từ offset đã commit.
5. Đối soát các chỉ số ở phần **Kiểm chứng**. Chỉ mở incident replay nếu freshness hoặc completeness vẫn không đạt.

## Quy trình replay/backfill an toàn

### 1. Xác định phạm vi và nguồn chuẩn

- Ghi rõ `REPLAY_RUN_ID`, UTC `START_TIME`, UTC `END_TIME`, lý do replay và người phê duyệt.
- Với dữ liệu còn Kafka retention, dùng Kafka offsets đã lưu hoặc offsets suy ra cho khoảng thời gian.
- Với dữ liệu đã ở Bronze, đọc Bronze Delta theo Delta version/time travel xác định. Không dùng `startingVersion=latest` cho backfill.

### 2. Dùng namespace tách biệt

Tạo toàn bộ path/checkpoint riêng cho run. Ví dụ:

```text
s3a://crypto-lake/replay/<REPLAY_RUN_ID>/bronze
s3a://crypto-lake/replay/<REPLAY_RUN_ID>/silver
s3a://crypto-lake/replay/<REPLAY_RUN_ID>/checkpoints/<stage>
```

Tạo bảng ClickHouse staging riêng, ví dụ `gold_crypto_analytics_replay_<REPLAY_RUN_ID>` và `crypto_price_alerts_replay_<REPLAY_RUN_ID>`. Không reuse checkpoint, output path hay bảng production.

### 3. Chạy theo thứ tự và đóng băng input

1. Replay Bronze cho đúng offset/version đã chọn.
2. Xác minh Bronze trước khi chạy Silver.
3. Chạy Silver với watermark phù hợp SLA late event, chờ tất cả cửa sổ của khoảng replay đóng.
4. Chạy Gold staging và chờ sink hoàn tất.
5. Lưu query progress, Delta history/version, checksum và kết quả đối soát làm evidence.

### 4. Promote có phê duyệt

Chỉ promote sau khi toàn bộ kiểm chứng đạt. Promote theo partition/window được phê duyệt, dùng merge/upsert theo `(symbol, window_start)` cho analytics và `alert_id` cho alerts. Sau promote, chạy lại đối soát production và lưu run ID trong change record.

## Kiểm chứng dữ liệu

Thực hiện cho toàn bộ replay window và theo từng `symbol`:

| Kiểm tra | Tiêu chí |
| --- | --- |
| Bronze completeness | Số `event_id` distinct không nhỏ hơn nguồn chuẩn, ngoại trừ bản ghi bị quarantine có lý do hợp lệ |
| Bronze uniqueness | `count(*) = countDistinct(event_id)` sau `FINAL`/dedupe logic |
| Silver uniqueness | Một dòng duy nhất cho mỗi `(symbol, window_start)` |
| Aggregate validity | `low_price <= vwap <= high_price`, `total_volume > 0`, `trade_count > 0` |
| Window coverage | Min/max `window_start` và số cửa sổ khớp phạm vi đã yêu cầu |
| Gold correctness | `price_change_pct` bằng thay đổi VWAP so với cửa sổ trước cùng symbol; dòng đầu phạm vi dùng seed window trước replay |
| Alert consistency | Mỗi Gold row vượt threshold có đúng một `alert_id` tất định; không có alert dưới threshold |
| Freshness | Dữ liệu mới nhất không vượt SLA đã công bố sau restart/promote |

Ví dụ truy vấn ClickHouse kiểm tra uniqueness Gold:

```sql
SELECT symbol, window_start, count() AS duplicate_count
FROM default.gold_crypto_analytics_v2 FINAL
GROUP BY symbol, window_start
HAVING duplicate_count > 1;
```

Ví dụ kiểm tra aggregate không hợp lệ ở Silver Delta (chạy trong Spark SQL):

```sql
SELECT *
FROM delta.`s3a://crypto-lake/silver_delta/crypto_trades_aggregated`
WHERE total_volume <= 0
   OR trade_count <= 0
   OR vwap < low_price
   OR vwap > high_price;
```

## Retention và bằng chứng audit

- Chỉ vacuum Delta sau retention window được phê duyệt cho replay, forensic và audit.
- Trước vacuum, lưu Delta history/version, checkpoint metadata, manifest file và evidence đối soát của các run còn trong retention.
- Lưu runbook execution record: run ID, code/image SHA, config không chứa secret, input offsets/Delta version, output version, kết quả đối soát, approver và thời điểm promote.
