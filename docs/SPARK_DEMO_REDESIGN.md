# Thiết kế lại demo Apache Spark cho hệ thống NIDS

## 1. Mục tiêu

Thiết kế lại demo của đề tài **Xây dựng hệ thống phát hiện xâm nhập mạng trên
dữ liệu lớn bằng Apache Spark MLlib** để người xem hiểu rõ:

- Apache Spark giải quyết phần nào của bài toán.
- Spark xử lý dữ liệu theo DataFrame, SQL, partition, task, stage và job ra sao.
- Lazy evaluation, shuffle, cache và Parquet có tác dụng gì.
- Spark MLlib xây dựng feature pipeline và huấn luyện Random Forest như thế nào.
- Spark Structured Streaming dùng model đã huấn luyện để dự đoán dữ liệu mới.

Dashboard là lớp trình bày kết quả. Spark Demo Runner và Spark UI mới là bằng
chứng Spark thực sự xử lý dữ liệu. Dashboard guided demo có 7 bước và không
điều khiển Spark subprocess.

## 2. Phạm vi và giả định

- Demo chạy trên laptop Windows bằng Spark local (`local[2]` hoặc `local[4]`).
- Dữ liệu chính là UNSW-NB15, khoảng 175.341 dòng train và 82.332 dòng test.
- Batch dùng để chuẩn bị dữ liệu và huấn luyện model.
- Streaming ban đầu dùng file Parquet theo micro-batch; Kafka là hướng mở rộng.
- Demo không bắt packet mạng trực tiếp và không được mô tả là IDS production.
- Dữ liệu công khai, không chứa dữ liệu cá nhân cần bảo vệ.
- Demo mode ưu tiên chạy ổn trong 8–10 phút; full mode tạo kết quả đồ án đầy đủ.
- Một người sở hữu và bảo trì project; cấu hình tập trung trong YAML.

## 3. Kiến trúc tổng thể

```text
                         BATCH PIPELINE
UNSW-NB15 CSV
      |
      v
Spark schema + validation
      |
      v
Bronze Parquet --> cleaning --> Silver Parquet --> Spark SQL EDA
                                           |
                                           v
                               Train / Validation / Test
                                           |
                                           v
                              Spark MLlib feature pipeline
                                           |
                                           v
                           Random Forest + threshold đã khóa
                                           |
                    +----------------------+------------------+
                    |                                         |
                    v                                         v
             Batch prediction                    Structured Streaming
                    |                              + checkpoint/restart
                    +----------------------+------------------+
                                           |
                                           v
                                outputs/demo + predictions
                                           |
                                           v
                                  Streamlit Dashboard

Trong khi runner hoạt động: Spark UI tại http://127.0.0.1:4040
```

## 4. Thành phần

### 4.1 Spark Demo Runner

Entry point là `python -m src.spark_demo`. Runner giữ một
SparkSession và chạy các thí nghiệm theo thứ tự xác định. Runner in giải thích
ngắn ra terminal, giữ Spark UI hoạt động và ghi artifact máy đọc được vào
`outputs/demo/`.

Runner không sửa hoặc đánh giá lại official test. Các run streaming có thư mục
và checkpoint riêng để tránh xung đột.

### 4.2 Dashboard

Dashboard chỉ đọc JSON/Parquet đã giới hạn. Nó không tự tạo SparkSession hoặc
khởi chạy subprocess Spark. Các màn hình được tổ chức theo câu hỏi:

1. **Dữ liệu và schema:** network flow là gì?
2. **CSV và Parquet:** Spark đọc hai format ra sao?
3. **Lazy evaluation:** vì sao action mới tạo Job?
4. **Job/Stage/Task:** partition, Exchange và shuffle liên hệ thế nào?
5. **Cache:** khi nào lưu dữ liệu trung gian có ích?
6. **Spark MLlib:** feature pipeline và Random Forest hoạt động ra sao?
7. **Structured Streaming:** micro-batch và checkpoint/restart thế nào?

### 4.3 Spark UI

Spark UI cung cấp bằng chứng kỹ thuật thật cho Jobs, Stages, SQL, Storage,
Executors và Environment. Dashboard chỉ giải thích các khái niệm đó theo cách
dễ hiểu.

## 5. Kịch bản demo 8–10 phút

1. **Bài toán (1 phút):** một dòng là một network flow; đầu ra là Normal/Attack.
2. **CSV và Parquet (1 phút):** chạy cùng truy vấn và so sánh thời gian, dung
   lượng, schema; xác nhận kết quả giống nhau.
3. **Lazy evaluation (1 phút):** khai báo `read -> filter -> groupBy` nhưng chưa
   có Job; action `collect()` mới kích hoạt Job.
4. **Partition, Task, Shuffle (1,5 phút):** thử 1/2/4/8 partition; chạy `groupBy`
   và xem Exchange/Shuffle trong plan và Spark UI.
5. **Cache (1 phút):** so sánh truy vấn lặp lại khi chưa cache và có cache; báo
   riêng chi phí materialize.
6. **Spark MLlib (2 phút):** trình bày StringIndexer, OneHotEncoder,
   VectorAssembler, StandardScaler và Random Forest. Demo mode train trên tập
   giới hạn; full mode dùng kết quả toàn bộ train.
7. **Structured Streaming (1,5 phút):** xử lý batch 0, batch 1, restart bằng
   checkpoint, sau đó chỉ xử lý batch 2 mới.
8. **Kết quả (30 giây):** xem cảnh báo, Recall, FPR và giới hạn tổng quát hóa.

## 6. Demo mode và full mode

### Demo mode

- Dữ liệu giới hạn và seed cố định.
- Mỗi thí nghiệm hoàn thành trong thời gian ngắn.
- Giữ SparkSession mở để xem Spark UI.
- Không chạy lại official-test evaluation.

### Full mode

- Dùng cùng entry point với cấu hình kích thước lớn hơn khi cần phát triển đồ án.
- Các metrics/model official đã xác minh vẫn nằm ở artifact Phase 6/7; guided
  demo không tự ý đánh giá lại official test.
- Có thể chạy pipeline đầy đủ trước buổi trình bày; dashboard dùng artifact đã
  xác minh hoặc status live của runner.

Hai chế độ dùng chung logic; chỉ khác cấu hình kích thước dữ liệu và thời gian
giữ SparkSession.

## 7. Các bằng chứng Spark bắt buộc

| Khái niệm | Bằng chứng trong demo |
|---|---|
| DataFrame | Schema, select, filter và aggregate trên UNSW-NB15 |
| Spark SQL | Thống kê Attack theo protocol/service |
| Lazy evaluation | Chưa có Job trước action; có Job sau action |
| Partition | Số partition và số task tương ứng trong Spark UI |
| Job/Stage/Task | Spark status tracker và Spark UI |
| Shuffle | Exchange trong physical plan của groupBy/orderBy |
| Cache | Storage tab và timing trước/sau cache |
| Parquet | Cùng kết quả với CSV, thời gian và dung lượng có điều kiện |
| MLlib | PipelineModel và RandomForestClassificationModel |
| Structured Streaming | Progress, batch ID, checkpoint và restart |

## 8. Xử lý lỗi và khả năng phục hồi

- Preflight kiểm tra config, dataset/model cần cho step và đường dẫn output.
- Mỗi bước ghi trạng thái `pending/running/succeeded/failed` bằng artifact riêng.
- Lỗi một thí nghiệm không xóa kết quả các thí nghiệm đã hoàn thành.
- Runner cung cấp thông báo dễ hiểu và đường dẫn log chi tiết.
- Streaming kiểm tra input/output/checkpoint không chồng đường dẫn.
- Output được ghi theo run ID và batch ID để retry không append bản sao tùy ý.
- Có artifact dự phòng đã tạo trước nếu máy trình bày gặp sự cố.

## 9. Kiểm thử và tiêu chí hoàn thành

- CSV và Parquet tạo cùng aggregate/hash kết quả.
- Transformation không kích hoạt Job trước action.
- Số partition và task được ghi nhận chính xác.
- Cache không làm thay đổi kết quả và báo riêng chi phí materialize.
- Feature vector không chứa `id`, `attack_cat` hoặc `label`.
- Demo mode load hoặc train đúng MLlib PipelineModel.
- Streaming restart chỉ bổ sung batch/file mới và không mất prediction.
- Dashboard render đủ bảy màn hình từ artifact giới hạn.
- Một lượt rehearsal hoàn thành trong 8–10 phút trên máy mục tiêu.

## 10. Hạn chế được công bố

- Local mode không chứng minh network shuffle giữa nhiều máy.
- UNSW-NB15 không phản ánh đầy đủ traffic và tấn công hiện đại.
- Streaming file-source không phải packet capture hoặc Kafka realtime.
- Model chỉ phân loại nhị phân và FPR official test còn cao.
- Benchmark nhỏ chỉ có ý nghĩa trên máy và cấu hình được ghi nhận.
- Hệ thống chưa có authentication, alert routing, drift monitoring hoặc SLA.

## 11. Hướng phát triển đồ án

```text
File source       -> Kafka/network-flow collector
Spark local       -> Standalone/YARN/Kubernetes cluster
Binary detection  -> Multi-class attack classification
Static model      -> Drift monitoring + retraining
Local artifacts   -> Database/data lake + alert service
Dashboard offline -> Near-real-time operations dashboard
```

## 12. Decision log

| Quyết định | Phương án khác | Lý do |
|---|---|---|
| Cải tiến repo hiện tại | Tạo demo mới độc lập | Tái sử dụng pipeline cho đồ án cuối kỳ |
| Batch training + streaming inference | Chỉ batch hoặc chỉ streaming | Phù hợp vòng đời ML và thể hiện nhiều thành phần Spark |
| Runner + Dashboard + Spark UI | Chỉ dashboard | Tách xử lý, giải thích và bằng chứng kỹ thuật |
| Dashboard không tự chạy Spark | Streamlit quản lý Spark subprocess | Ổn định hơn trên Windows và tránh nhiều SparkSession |
| Bổ sung lazy evaluation và cache | Chỉ benchmark CSV/Parquet | Thể hiện đặc trưng cốt lõi của Spark |
| Có demo mode và full mode | Chỉ chạy full pipeline | Demo nhanh nhưng vẫn dùng chung code đồ án |
| File-source trước, Kafka sau | Kafka ngay lập tức | Giảm rủi ro và độ phức tạp ở phiên bản đầu |
