# Báo cáo kỹ thuật — Phát hiện xâm nhập mạng với Apache Spark MLlib

**Phiên bản:** Phase 11, 2026-09-10  
**Phạm vi:** phân loại nhị phân Normal/Attack trên UNSW-NB15 bằng PySpark
DataFrame, Spark SQL và Spark MLlib. Đây là đồ án nghiên cứu batch/local; không
phải IDS production hay hệ thống bắt packet trực tiếp.

## 1. Bài toán và mục tiêu

Lưu lượng mạng có nhiều trường số, categorical và nhãn tấn công; kiểm tra thủ
công không khả thi khi quy mô tăng. Đồ án trả lời ba câu hỏi:

1. Pipeline Spark xử lý, chuẩn hóa và phân vùng network flow như thế nào?
2. Logistic Regression và Random Forest khác nhau thế nào về Recall, FPR, F1
   và ranking metrics?
3. Parquet, cache và số partition ảnh hưởng ra sao trên cấu hình local hiện có?

Sản phẩm gồm pipeline batch có thể chạy lại, PipelineModel đã khóa/nạp lại,
prediction có xác suất, dashboard offline, benchmark Spark và mô phỏng file
source Structured Streaming.

## 2. Dữ liệu và phạm vi

Nguồn chuẩn là [UNSW-NB15 của UNSW](https://research.unsw.edu.au/projects/unsw-nb15-dataset).
Hai designated split được giữ nguyên vai trò: train **175.341** record và test
**82.332** record. Chi tiết checksum/mirror có kiểm soát nằm tại
docs/DATASET_SOURCE.md; schema 45 cột và vai trò từng nhóm cột tại
docs/DATA_DICTIONARY.md.

Nhãn chính là label (0=Normal, 1=Attack). attack_cat chỉ dùng EDA và phân tích
lỗi, còn id chỉ là định danh; hai cột này tuyệt đối không vào feature vector.
Train có 68,06% Attack nên Accuracy không được dùng độc lập để kết luận.

## 3. Kiến trúc và data lineage

~~~text
UNSW-NB15 CSV (schema tường minh, checksum)
        |
        v
Bronze Parquet --> validate --> Silver Parquet --> Spark SQL EDA
        |                                      |
        |                                      v
        |                         split theo hash(id, seed=42)
        |                                      |
        |                        modeling train / validation
        |                                      |
        |          StringIndexer + OHE + StandardScaler + VectorAssembler
        |                                      |
        |                  LR baseline + bounded Random Forest comparison
        |                                      |
        |                  threshold chọn trên validation = 0.60
        |                                      |
        +------------------------------> locked PipelineModel (SHA-256)
                                               |
                        +----------------------+---------------------+
                        |                                            |
                  batch Parquet inference                    file-source stream
                        |                                            |
             metrics/predictions/dashboard                 checkpoint + batches
~~~

Mọi đường dẫn, seed, partition và threshold được tập trung trong
configs/default.yaml. Bronze/Silver lưu Parquet Snappy. Dashboard chỉ đọc JSON
aggregate hoặc sample prediction đã giới hạn, không khởi động Spark.

## 4. Phương pháp và kiểm soát leakage

- Schema StructType được khai báo trước khi đọc CSV, không dùng inferSchema.
- Làm sạch categorical thiếu bằng token __unknown__; kiểm tra null, kiểu, trùng
  lặp, miền nhãn và số không âm theo contract.
- Chia train/validation xác định bằng pmod(xxhash64(id, seed), 10000): training
  140.471 dòng, validation 34.870 dòng, không giao ID, official test chưa được
  mở trong lúc chọn mô hình.
- StringIndexer, OneHotEncoder, StandardScaler, assembler và classifier cùng
  nằm trong Spark Pipeline, chỉ fit trên modeling train.
- Chọn candidate/threshold theo F1 validation (tie-break: Recall cao hơn, FPR
  thấp hơn) trước lần đánh giá test duy nhất. Phase 7 bảo vệ không cho chạy lại
  evaluation official test vô tình.

## 5. EDA ngắn gọn

Trên Silver train, các nhóm lớn là Normal 56.000, Generic 40.000, Exploits
33.393 và Fuzzers 18.184. TCP có 79.946 flow; UDP có attack rate 78,00%; service
__unknown__ chiếm 94.168 flow. Các tỷ lệ này là mô tả tập dữ liệu, **không phải
feature importance hoặc quan hệ nhân quả**. Bảng/biểu đồ đầy đủ ở
docs/EDA_REPORT.md.

## 6. Mô hình và lựa chọn trên validation

| Candidate | Threshold | Precision | Recall | F1 | FPR | ROC-AUC | PR-AUC |
|---|---:|---:|---:|---:|---:|---:|---:|
| Logistic Regression | 0,55 | 0,9270 | 0,9825 | 0,9540 | 0,1662 | 0,9841 | 0,9916 |
| LR balanced | 0,40 | 0,9337 | 0,9756 | 0,9542 | 0,1489 | 0,9842 | 0,9919 |
| Random Forest depth 8 | 0,65 | 0,9280 | 0,9884 | 0,9573 | 0,1648 | 0,9840 | 0,9921 |
| **Random Forest depth 12** | **0,60** | **0,9472** | **0,9777** | **0,9622** | **0,1172** | **0,9902** | **0,9953** |

Random Forest 50 cây, độ sâu 12 được khóa vì có F1 validation cao nhất và FPR
thấp hơn các candidate còn lại tại threshold đã chọn. Chi tiết hyperparameter,
thời gian và đường threshold ở docs/MODEL_COMPARISON.md.

## 7. Đánh giá official test duy nhất

Sau khi khóa model/hash/threshold từ validation, PipelineModel được nạp trong
tiến trình khác và transform 82.332 record test. Kết quả tại threshold 0,60:

| Chỉ số | Giá trị |
|---|---:|
| Precision | 0,805796 |
| Recall | 0,987514 |
| F1 | 0,887448 |
| FPR | 0,291595 |
| ROC-AUC | 0,978606 |
| PR-AUC | 0,983752 |
| TN / FP / FN / TP | 26.211 / 10.789 / 566 / 44.766 |

Recall và AUC vẫn cao, nhưng FPR test 29,16% cao hơn validation 11,72%. Đây là
kết quả tổng quát hóa cần báo cáo trung thực; **không** chỉnh lại threshold sau
khi thấy test. docs/FINAL_TEST_REPORT.md và outputs/metrics/phase7_final_test.json
là artifact nguồn cho các số liệu này.

## 8. Bằng chứng Apache Spark

Truy vấn đại diện filter -> groupBy(label, proto) -> count/sum -> orderBy trên
175.341 dòng ghi nhận 4 Job, 8 stage ID dưới AQE, 7 task hoàn tất và 0 task lỗi.
Physical plan có 4 Exchange, biểu hiện shuffle ở aggregate/order.

~~~text
partition đầu vào -> task đọc/filter -> Exchange (shuffle)
  -> task aggregate -> Exchange/order -> task kết quả -> action collect aggregate nhỏ
~~~

Trên Windows 11, Python 3.12.13, Java 21.0.12.1, Spark 4.2.0, local[2], driver
2 GB và 4 shuffle partitions:

| Phép đo | Kết quả có điều kiện |
|---|---|
| CSV vs Parquet | Parquet median 0,1981s; CSV 0,3288s; Parquet nhanh 1,66x và nhỏ hơn 2,45x |
| Cache | Reuse/action nhanh 1,16x, nhưng cần khoảng 37 reuse để hòa vốn materialize |
| Partition | Aggregate nhỏ tốt nhất tại 1; materialize tốt nhất tại 4 |

Đây là benchmark local ba lần sau warmup, không suy rộng thành kết luận cluster.
Xem plan, protocol và giới hạn tại docs/SPARK_EXECUTION_AND_BENCHMARK.md.

## 9. Inference, dashboard và streaming

Batch inference nạp model có SHA-256 750231e4…4f93a2f, xuất Parquet/CSV có
probability, threshold, nhãn, thời điểm và run ID. Dashboard Streamlit có bốn
trang (overview/model/alerts/benchmark), chỉ dùng aggregate và tối đa 200 cảnh
báo để demo offline: docs/DASHBOARD_GUIDE.md.

Phase 10 là phần mở rộng: Parquet file source có schema cố định,
maxFilesPerTrigger=1, AvailableNow và checkpoint. Demo đã xử lý 3 file × 20
dòng qua hai Spark application; persistent query ID giữ nguyên, chỉ batch 2 được
xử lý sau restart, đủ 60 ID phân biệt. foreachBatch có at-least-once semantics;
ghi output theo batch_id giảm nguy cơ duplicate khi retry nhưng chưa thay thế
sink giao dịch production. Xem docs/STREAMING_GUIDE.md.
Artifact nguồn của demo checkpoint/restart là outputs/metrics/phase10_streaming.json.

## 10. Hạn chế và hướng phát triển

- UNSW-NB15 là dữ liệu nghiên cứu có nhãn, không phản ánh trọn vẹn traffic/live
  threat hiện đại; chưa có packet capture trực tiếp.
- Official-test FPR cao; cần kiểm tra shift dữ liệu, chi phí FP/FN và đánh giá
  theo môi trường trước khi triển khai thực tế.
- Benchmark là local local[2], không đo nhiều executor, network shuffle hoặc
  dữ liệu nhiều triệu dòng thực sự trong cluster.
- Streaming chỉ là file micro-batch mô phỏng; production cần ingestion an toàn,
  observability, xác thực input, sink giao dịch, phân quyền và monitoring drift.
- Dashboard không phải bảng điều khiển an ninh thời gian thực; alert list là
  sample giới hạn.

## 11. Khả năng tái lập và nghiệm thu

1. Chạy .\scripts\bootstrap.ps1 trên PowerShell từ project root.
2. Kiểm thử: .\.venv\Scripts\python.exe -m pytest -q.
3. Dashboard: .\.venv\Scripts\streamlit.exe run dashboard\app.py.
4. Streaming demo: .\.venv\Scripts\python.exe -m src.streaming_predict.

Kịch bản trình bày 5–8 phút, checklist màn hình và phương án dự phòng tại
docs/KICH_BAN_DEMO.md. Nhật ký đầy đủ, lệnh và kết quả theo phase tại
docs/NHAT_KY_THUC_HIEN.md.
