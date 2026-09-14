# Hướng dẫn guided demo Apache Spark

Dashboard mới là màn hình thuyết trình cho đề tài **Xây dựng hệ thống phát hiện
xâm nhập mạng trên dữ liệu lớn bằng Apache Spark MLlib**. Nó không bắt packet
trực tiếp và không phải IDS production. Mục tiêu của dashboard là giúp người
xem nhìn thấy rõ Spark nhận dữ liệu gì, xử lý gì và tạo ra kết quả gì.

## Chạy demo

Mở PowerShell tại thư mục:

```powershell
D:\Learning\bigdata\SparkNIDS-Spark-Network-Intrusion-Detection-System
```

Terminal 1 chạy Spark runner:

```powershell
.\scripts\run_demo.ps1
```

Runner sẽ ghi trạng thái vào `outputs/demo/`, giữ Spark UI sống trong thời gian
được cấu hình và in URL Spark UI. Có thể chạy riêng một bước khi cần:

```powershell
.\.venv\Scripts\python.exe -m src.spark_demo --step execution --hold-seconds 120
```

Terminal 2 chạy giao diện:

```powershell
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Mở:

- Dashboard: `http://127.0.0.1:8501`.
- Spark UI: `http://127.0.0.1:4040` trong lúc runner còn giữ SparkSession.

Spark UI sẽ biến mất hoặc không còn dữ liệu truy cập được sau khi runner dừng
SparkSession. Đây là hành vi bình thường của Spark local, không phải lỗi
dashboard.

## Bảy bước trên màn hình

Dashboard có một thanh tiến trình ngang và nút `Quay lại`/`Tiếp theo`. Mỗi bước
chỉ có một flow trung tâm:

`Input → Spark xử lý → Kết quả`

1. **Dữ liệu và schema:** một dòng là network flow; label `0` là Normal, label
   `1` là Attack; Spark DataFrame giữ schema 45 cột.
2. **CSV và Parquet:** cùng một truy vấn aggregate được chạy trên hai format;
   hash kết quả phải giống nhau, thời gian và dung lượng được đo trong điều
   kiện hiện tại.
3. **Lazy evaluation:** `filter`, `groupBy`, `orderBy` chỉ dựng kế hoạch; gọi
   `collect()` mới tạo Job.
4. **Job, Stage, Task:** xem số partition, status tracker và `Exchange` trong
   physical plan để nối khái niệm Spark với Spark UI.
5. **Cache:** tách chi phí materialize cache khỏi chi phí các lần reuse; cache
   chỉ có lợi nếu cùng dữ liệu được dùng lại đủ nhiều.
6. **MLlib Random Forest:** feature pipeline biến cột thành vector, sau đó
   Random Forest phân loại Normal/Attack trên sample modeling.
7. **Structured Streaming:** file Parquet đến theo micro-batch; checkpoint
   giúp restart chỉ đọc file mới.

Phần “Bằng chứng Spark” chỉ hiển thị các thông tin nhỏ như plan, Job/Stage/Task,
pipeline stages, batch ID và restart checks. Dashboard không import PySpark,
không đọc Bronze/Silver lớn và không gọi `toPandas()`.

## Live artifact và fallback offline

Runner ghi:

- `outputs/demo/latest.json`: con trỏ tới run mới nhất.
- `outputs/demo/runs/<run_id>/status.json`: trạng thái từng bước.
- `outputs/demo/runtime/<run_id>/`: runtime streaming của run đó.

Nếu chưa chạy runner, dashboard dùng các artifact Phase 3/6/8/10 đã export để
trình bày offline. Nếu một status đang được thay thế hoặc một step lỗi, giao
diện vẫn mở và hiển thị trạng thái/cách khắc phục thay vì crash.

## Giới hạn cần nói rõ

- Runner hiện dùng `local[2]`, tức một máy và một local executor; chưa chứng
  minh network shuffle giữa nhiều worker.
- CSV/Parquet, partition và cache chỉ được benchmark trên workload/máy hiện
  tại; không được kết luận Spark luôn nhanh hơn trong mọi trường hợp.
- Streaming là file-source mô phỏng micro-batch, chưa phải packet capture hoặc
  Kafka realtime.
- MLlib demo dùng sample để trình bày; official test được giữ riêng và không
  chạy lại trong guided demo.
- Model cuối vẫn là nghiên cứu trên UNSW-NB15; FPR official test cao nên chưa
  được dùng làm bộ chặn production.

## Kiểm tra

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_dashboard.py -q
.\.venv\Scripts\python.exe -m pytest tests\test_spark_demo.py -q
```
