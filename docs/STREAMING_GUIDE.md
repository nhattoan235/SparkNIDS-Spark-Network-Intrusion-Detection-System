# Structured Streaming file-source — Phase 10

Phase này mô phỏng lưu lượng đến theo từng file Parquet. Nó dùng Spark
Structured Streaming micro-batch, không bắt packet trực tiếp, không dùng Kafka
và không phải một IDS production.

## Kiến trúc

```text
UNSW-NB15 sample
      |
      | ghi staging rồi atomic move
      v
Parquet file source -- readStream(schema cố định, 1 file/trigger)
      |
      v
foreachBatch --> load PipelineModel đã khóa --> probability --> threshold 0.60
      |                                      |
      |                                      +--> summary số cảnh báo/batch
      +--> prediction Parquet theo batch_id

Checkpoint: offsets + commits + persistent query ID
```

PipelineModel Phase 7 được xác minh SHA-256 và nạp từ đĩa ở mỗi Spark
application. Streaming không gọi `fit()` và không chọn lại threshold.

## Demo checkpoint/restart đã đóng gói

```powershell
.\.venv\Scripts\python.exe -m src.streaming_predict
```

Mỗi lần gọi tự tạo một tên run mới để không ghi đè bằng chứng cũ. Quy trình:

1. Chọn 60 record cố định từ sample và công bố hai file đầu, mỗi file 20 dòng.
2. `AvailableNow` xử lý backlog thành batch 0 và 1 vì
   `maxFilesPerTrigger=1`.
3. Dừng SparkSession thứ nhất.
4. Tạo SparkSession mới, công bố file thứ ba và khởi động query với checkpoint
   cũ.
5. Xác minh persistent query ID không đổi, query run ID đổi, chỉ batch 2 được
   thêm, đủ 60 prediction/60 ID phân biệt và ba summary liên tiếp.

Artifact mới nhất:

- `outputs/metrics/phase10_streaming.json`: cấu hình, progress từng trigger và
  toàn bộ checkpoint/restart checks.
- `outputs/streaming/<run>/predictions/batch-*`: prediction có xác suất, nhãn,
  model hash, source file và batch ID.
- `outputs/streaming/<run>/batch_summaries/batch-*`: số record, số cảnh báo,
  alert rate và thống kê xác suất theo batch.
- `outputs/streaming/<run>/checkpoint`: offsets/commits dùng khi restart.

## Chạy với thư mục nguồn riêng

```powershell
.\.venv\Scripts\python.exe -m src.streaming_predict `
  --input-dir data\streaming_input\manual `
  --output-dir outputs\streaming\manual `
  --checkpoint-dir outputs\streaming\manual\checkpoint `
  --query-name unsw-nb15-manual
```

Thêm file Parquet mới bằng staging rồi rename/move nguyên tử vào thư mục nguồn,
sau đó gọi lại cùng lệnh. Không đổi query name, checkpoint, schema hoặc cấu hình
partition của một query đang được khôi phục.

Input phải khớp `UNSW_NB15_SCHEMA` trong `src/unsw_nb15_schema.py`. Các trường
`id`, 39 numeric features, `proto`, `service`, `state`, `attack_cat` và `label`
được đọc theo schema tường minh. Đây là mô phỏng từ dữ liệu có nhãn; inference
không dùng `attack_cat` hoặc `label` làm feature.

## Delivery semantics và giới hạn

Spark quy định `foreachBatch` có ngữ nghĩa at-least-once. Implementation này
ghi mỗi batch vào thư mục xác định bởi `batch_id` với mode overwrite, vì vậy một
batch retry sẽ thay đúng output của nó thay vì append bản sao. Đây chưa phải
giao dịch phân tán giữa prediction và summary; production cần sink giao dịch
hoặc cơ chế commit ngoài.

`AvailableNow` xử lý toàn bộ file đã có tại lúc query bắt đầu rồi tự dừng. Chế
độ này phù hợp demo và cron nhỏ, không đồng nghĩa latency real-time. Các cảnh
báo Hadoop native/large task binary trên Windows local không làm query thất bại
nhưng cần được cân nhắc khi chuyển sang cluster.

## Kiểm thử

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_streaming.py -q
```

Test xác nhận readStream dùng schema cố định, aggregate cảnh báo đúng và source,
sink, checkpoint không được chồng đường dẫn.
