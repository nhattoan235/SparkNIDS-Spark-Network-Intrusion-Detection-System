# Nhật ký thực hiện đồ án

Nhật ký này là nguồn theo dõi trạng thái theo phase. Chỉ đánh dấu hoàn thành khi
có lệnh kiểm thử và kết quả thực tế tương ứng.

## Tổng quan trạng thái

| Phase | Trạng thái | Ghi chú |
|---|---|---|
| 0 — Chốt yêu cầu và tiêu chí | Hoàn thành theo kế hoạch nguồn | Tên đề tài, phạm vi batch và đầu ra bắt buộc đã được chốt trong kế hoạch; yêu cầu riêng của giảng viên chưa được cung cấp. |
| 1 — Môi trường và bộ khung | Hoàn thành | Spark local và test đều chạy thành công. |
| 2 — Thu nhận và chuẩn hóa dữ liệu | Hoàn thành | CSV đã xác minh; Bronze Parquet và sample đọc lại thành công. |
| 3 — Làm sạch và EDA bằng Spark | Hoàn thành | Silver ổn định; 6 truy vấn SQL và 4 biểu đồ đã sinh. |
| 4 — Xây dựng Spark ML Pipeline | Hoàn thành | Pipeline fit/transform thành công; split chống leakage và 3 test phase đều đạt. |
| 5 — Baseline Logistic Regression | Hoàn thành | Baseline và 17 threshold validation đã đánh giá; threshold mặc định 0,55. |
| 6 — Mô hình so sánh và xử lý mất cân bằng | Hoàn thành | So sánh 4 candidate; chọn Random Forest depth 12 tại threshold 0,60. |
| 7 — Đánh giá cuối, lưu model và batch inference | Hoàn thành | Model khóa/lưu/nạp thành công; official test đánh giá đúng một lần và đã xuất prediction. |
| 8 — Chứng minh giá trị của Apache Spark | Hoàn thành | Có plan, Job/Stage/Task evidence và benchmark CSV/Parquet/cache/partition tái lập. |
| 9 — Dashboard trình bày | Hoàn thành | Dashboard Streamlit offline có 4 trang, chỉ đọc aggregate và prediction đã giới hạn; đã kiểm tra bằng AppTest và trình duyệt thật. |
| 10 — Structured Streaming tùy chọn | Hoàn thành | File-source micro-batch có schema cố định, nạp PipelineModel đã khóa, checkpoint và restart verification. |
| 11 — Báo cáo và chuẩn bị thuyết trình | Hoàn thành | Báo cáo kỹ thuật, kiến trúc/lineage, kịch bản demo 5–8 phút, checklist và phương án offline đã hoàn thiện. |

## 2026-09-10 09:04 +07:00 — Phase 1: Môi trường và bộ khung

### Việc đã làm

- Đọc toàn bộ `KE_HOACH_PHAT_HIEN_XAM_NHAP_SPARK.md` và kiểm kê workspace.
- Xác nhận workspace ban đầu chỉ có file kế hoạch, không có Git repository,
  README, source code, dữ liệu hay nhật ký cũ cần bảo toàn.
- Kiểm tra môi trường ban đầu:
  - Java hệ thống: Oracle Java `26.0.1`; chưa có `JAVA_HOME`.
  - Python, PySpark và `spark-submit`: chưa có trên `PATH`.
  - `uv 0.11.28` có sẵn.
  - 12 logical processors; ổ D còn khoảng 35.86 GB. Truy vấn RAM bị môi trường
    thực thi từ chối quyền nên chưa ghi nhận được dung lượng RAM hệ thống.
- Tạo Python `3.12.13` trong `.venv` và cài PySpark `4.2.0`, PyYAML `6.0.3`,
  NumPy `2.5.3`, pytest `8.4.2`.
- Phát hiện Java 26 không tương thích trong lần chạy thật
  (`ClassNotFoundException: jdk.internal.ref.Cleaner`), sau đó cài Eclipse
  Temurin JDK `21.0.12.1` dạng portable trong `.jdk` và cấu hình dự án tự ưu tiên
  JDK này. Không thay đổi Java toàn hệ thống.
- Tạo cấu trúc thư mục theo kế hoạch, file ignore cho dữ liệu/model/output lớn,
  cấu hình YAML tập trung, SparkSession local và smoke test DataFrame.
- Tạo script bootstrap môi trường và script nạp biến môi trường cho
  `spark-submit` trên PowerShell.
- Viết README ban đầu với đúng các lệnh đã được kiểm chứng.

### File đã tạo hoặc thay đổi

- `.gitignore`
- `README.md`
- `requirements.txt`
- `configs/default.yaml`
- `scripts/bootstrap.ps1`
- `scripts/project_env.ps1`
- `src/__init__.py`
- `src/spark_session.py`
- `tests/test_spark_session.py`
- Các `.gitkeep` trong `data/`, `models/`, `outputs/`, `dashboard/` và `docs/`.
- `docs/NHAT_KY_THUC_HIEN.md`

Runtime cục bộ không commit: `.venv/`, `.uv-python/`, `.uv-cache/`, `.jdk/` và
`.downloads/`.

### Lệnh kiểm thử và kết quả

1. Smoke test bằng Python tương đương `spark-submit`:

   ```powershell
   .\.venv\Scripts\python.exe -m src.spark_session
   ```

   Kết quả: exit code `0`; Spark `4.2.0`, master `local[2]`, 2 input
   partitions, action đếm đúng 3 dòng; tổng hợp đúng `Attack=2` và `Normal=1`;
   Spark UI được tạo tại `http://127.0.0.1:4040` trong thời gian session chạy.

2. Bộ test phase 1:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả: exit code `0`, `3 passed in 21.53s`.

3. Kiểm tra launcher Spark:

   ```powershell
   . .\scripts\project_env.ps1
   spark-submit --version
   ```

   Kết quả: exit code `0`; Spark `4.2.0`, Scala `2.13.18`, OpenJDK
   `21.0.12.1`.

### Vấn đề còn lại

- Spark trên Windows cảnh báo không có `winutils.exe` và native Hadoop library;
  các DataFrame action hiện vẫn chạy đúng. Không tải binary `winutils.exe` từ
  nguồn không chính thức ở phase này.
- Runner sandbox in thêm `ERROR: Access denied` khi dọn tiến trình JVM sau khi
  Spark đã dừng; cả smoke test và pytest vẫn trả exit code `0`. Cần kiểm tra lại
  trên PowerShell người dùng nếu lỗi này ảnh hưởng các job ghi file ở phase 2.
- Chưa có Git repository nên hiện không có lịch sử commit hay trạng thái diff.
- Chưa có dữ liệu UNSW-NB15. Không sử dụng dữ liệu mô phỏng thay dữ liệu nghiên
  cứu thật.

### Phase tiếp theo

Phase 2 — Thu nhận, kiểm tra và chuẩn hóa dữ liệu: xác minh nguồn tải chính thức,
xây schema tường minh, ingest train/test UNSW-NB15, sinh validation report,
Bronze Parquet và sample test cố định.

## 2026-09-10 19:41 +07:00 — Phase 2: Thu nhận và chuẩn hóa dữ liệu

### Việc đã làm

- Xác minh trang nguồn UNSW chính thức và số bản ghi công bố: 175.341 train,
  82.332 test. Liên kết tải chính thức hiện chuyển sang SharePoint yêu cầu đăng
  nhập trong môi trường này.
- Tải mirror của đúng hai designated split và ghi rõ đây không phải nguồn chính
  thức. Xác minh SHA-256 trước khi xử lý:
  - Train: `bec7dd5ec88dc2a0ccc7a07879d338395ed7421750f675fd0339e07dfe0648fa`.
  - Test: `734fe6642edf758f7c94d7d9149426b49d202fe8e7bf0bef47392489c3c0a559`.
- Tạo downloader có kiểm tra checksum, không tự ghi đè file sai checksum nếu
  không có `--force`.
- Khai báo `StructType` tường minh cho toàn bộ 45 cột; không dùng
  `inferSchema`. Tập trung danh sách cột số, categorical, identifier và target.
- Validation bằng Spark gồm header, checksum, row/column count, null từng cột,
  trùng lặp, label ngoài 0/1, phân phối label và `attack_cat`.
- Ghi Snappy Parquet vào `data/bronze/train` và `data/bronze/test`, sau đó đọc
  lại để đối chiếu schema và số dòng.
- Sinh sample CSV cố định 200 dòng từ train, gồm 100 Normal và 100 Attack, dùng
  cho test mà không cần tải dataset đầy đủ.
- Phát hiện Hadoop 3.5 trên Windows không ghi local filesystem nếu thiếu
  `winutils.exe`. Thay vì dùng native binary không chính thức, tích hợp
  `hadoop-bare-naked-local-fs 0.1.0` từ Maven Central, kiểm tra SHA-1
  `cd03dc0f6e2b8d8957d97d421e95d9ceaa16b06b`, và nạp qua driver classpath.
- Cập nhật bootstrap, README, tài liệu provenance, data dictionary và báo cáo
  validation.

### File đã tạo hoặc thay đổi

- `.gitignore`
- `README.md`
- `configs/default.yaml`
- `scripts/bootstrap.ps1`
- `src/spark_session.py`
- `src/download_data.py`
- `src/unsw_nb15_schema.py`
- `src/validate_data.py`
- `src/ingest.py`
- `tests/test_validation.py`
- `docs/DATASET_SOURCE.md`
- `docs/DATA_DICTIONARY.md`
- `docs/DATA_VALIDATION.md`
- `docs/NHAT_KY_THUC_HIEN.md`
- Sinh cục bộ, không commit: `data/raw/*.csv`, `data/bronze/*`,
  `outputs/metrics/phase2_validation.json`, `.spark-jars/*`.
- Sample nhỏ được phép lưu: `data/samples/unsw_nb15_sample_csv/`.

### Lệnh kiểm thử và kết quả

1. Xác minh downloader idempotent:

   ```powershell
   .\.venv\Scripts\python.exe -m src.download_data
   ```

   Kết quả: exit code `0`; cả hai file hiện có được xác minh checksum, không tải
   lại và không ghi đè.

2. Ingest/validation/Bronze end-to-end:

   ```powershell
   .\.venv\Scripts\python.exe -m src.ingest
   ```

   Kết quả: exit code `0`, report `status=ok`:

   | Split | Rows | Columns | Nulls | Duplicates | Invalid labels | Bronze rows |
   |---|---:|---:|---:|---:|---:|---:|
   | train | 175.341 | 45 | 0 | 0 | 0 | 175.341 |
   | test | 82.332 | 45 | 0 | 0 | 0 | 82.332 |

   Phân phối nhãn: train có 119.341 Attack/56.000 Normal; test có 45.332
   Attack/37.000 Normal. Sample có 200 dòng cân bằng.

3. Regression và phase tests:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả: exit code `0`, `6 passed in 25.32s`.

### Vấn đề còn lại

- Trang UNSW là nguồn chuẩn nhưng tải file qua SharePoint hiện cần đăng nhập;
  mirror và checksum được ghi minh bạch trong `docs/DATASET_SOURCE.md`. Có thể
  thay hai CSV bằng file tải thủ công chính thức nếu checksum vẫn khớp.
- Hadoop vẫn in cảnh báo khởi tạo rằng không thấy `winutils.exe`, nhưng filesystem
  Java thuần đã ghi/đọc Parquet thành công. Cảnh báo không còn chặn pipeline.
- Runner sandbox vẫn in `ERROR: Access denied` lúc dọn tiến trình JVM sau khi
  Spark đã trả kết quả; các lệnh đều exit `0` và output đã được xác minh.
- Raw CSV khoảng 47,67 MB và Bronze khoảng 19,90 MB chỉ tồn tại cục bộ, đã được
  `.gitignore` loại khỏi commit.

### Phase tiếp theo

Phase 3 — Làm sạch và EDA bằng Spark: chuẩn hóa categorical placeholder, kiểm
tra miền giá trị và số thực không hữu hạn, tạo Silver Parquet, chạy ít nhất ba
truy vấn Spark SQL, xuất bảng phân phối/biểu đồ nhỏ và giải thích mất cân bằng.

## 2026-09-10 20:01 +07:00 — Phase 3: Làm sạch và EDA bằng Spark

### Việc đã làm

- Xây cleaning pipeline tách biệt Bronze/Silver, giữ nguyên hợp đồng 45 cột và
  kiểu dữ liệu tường minh.
- Thiết lập quy tắc kiểm tra/xử lý:
  - Loại dòng có `id`, `label` hoặc quan hệ `label`–`attack_cat` không hợp lệ.
  - Loại bản ghi trùng toàn bộ 45 cột.
  - Đưa null, NaN, ±∞ và số âm ở numeric feature về 0 theo đúng kiểu cột.
  - Trim/lowercase `proto`, `service`, `state`; chuẩn hóa `-`, rỗng hoặc null
    thành `__unknown__`.
  - Chuẩn hóa cách viết các giá trị `attack_cat`, nhưng không dùng cột này làm
    feature cho bài toán nhị phân.
- Audit trước/sau cleaning cho cả train/test. Dữ liệu gốc không có numeric bất
  hợp lệ, target sai hoặc bản ghi trùng; không dòng nào bị loại. Thay đổi thực
  tế là 94.168 giá trị service train và 47.153 giá trị service test từ `-` thành
  `__unknown__`.
- Ghi và đọc lại Silver Snappy Parquet:
  - Train: 175.341 dòng.
  - Test: 82.332 dòng.
  - Schema kiểu dữ liệu khớp 45 cột và audit sau làm sạch không còn lỗi.
- Chạy EDA chỉ trên `silver/train` để tránh dùng đặc trưng test cho quyết định
  mô hình. Sáu truy vấn Spark SQL đã chạy:
  `class_distribution`, `attack_category_distribution`,
  `protocol_attack_rate`, `service_attack_rate`, `state_attack_rate`,
  `numeric_profile_by_class`.
- Tính Pearson correlation giữa từng numeric feature và label bằng Spark;
  `sttl` cao nhất theo trị tuyệt đối (`0.6927`), sau đó `ct_state_ttl`
  (`0.5777`) và `dload` (`-0.3937`). Đây chỉ là EDA đơn biến, không phải feature
  importance hay bằng chứng quan hệ nhân quả.
- Sinh bốn biểu đồ SVG từ các bảng aggregate nhỏ: phân phối lớp, attack category,
  protocol phổ biến và tương quan numeric-label.
- Xuất aggregate riêng cho dashboard tương lai. Không dùng `toPandas()` và
  không collect toàn bộ dataset.
- Bổ sung kiểm thử với dữ liệu bẩn giả lập và kiểm thử truy vấn SQL có giới hạn.

### File đã tạo hoặc thay đổi

- `configs/default.yaml`
- `README.md`
- `src/preprocess.py`
- `src/eda.py`
- `tests/test_preprocess.py`
- `docs/EDA_REPORT.md`
- `docs/NHAT_KY_THUC_HIEN.md`
- Sinh cục bộ, không commit:
  - `data/silver/train/`, `data/silver/test/` — tổng khoảng 21,26 MB.
  - `outputs/metrics/phase3_cleaning.json`.
  - `outputs/metrics/phase3_eda.json`.
  - `outputs/dashboard/eda_summary.json`.
  - `outputs/figures/*.svg` — 4 file, tổng khoảng 7,8 KB.

### Lệnh kiểm thử và kết quả

1. Test chuyên biệt cleaning và Spark SQL:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest tests\test_preprocess.py -q
   ```

   Kết quả: exit code `0`, `2 passed in 64.84s`.

2. Chạy cleaning end-to-end:

   ```powershell
   .\.venv\Scripts\python.exe -m src.preprocess
   ```

   Kết quả: exit code `0`, `status=ok`; Silver train/test đọc lại lần lượt
   175.341/82.332 dòng, không còn giá trị bị audit là không hợp lệ.

3. Chạy EDA Spark SQL end-to-end:

   ```powershell
   .\.venv\Scripts\python.exe -m src.eda
   ```

   Kết quả: exit code `0`, `status=ok`; 6 truy vấn, 175.341 dòng train được xử
   lý, 4 SVG hợp lệ được sinh. Tỷ lệ lớp lớn/lớp nhỏ `2.1311:1`.

4. Toàn bộ regression suite:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả xác minh cuối sau mọi thay đổi: exit code `0`, `8 passed in 72.51s`.

### Kết quả EDA chính

- Attack: 119.341 dòng (`68,0622%`); Normal: 56.000 dòng (`31,9378%`). Accuracy
  của mô hình chỉ đoán Attack có thể đạt khoảng 68%, nên không đủ làm kết luận.
- TCP có 79.946 flow với attack rate `51,07%`; UDP có 63.283 flow với attack
  rate `78,00%`; `unas` có 12.084 flow và toàn bộ mang nhãn Attack trong train.
- State `int` có attack rate `93,05%`, trong khi `con` chỉ `8,01%`.
- Worms chỉ có 130 dòng (`0,0741%`) trong train, thể hiện mất cân bằng rất mạnh
  ở mức attack category dù bài toán nhị phân chỉ mất cân bằng khoảng 2,13:1.
- Báo cáo đầy đủ: `docs/EDA_REPORT.md`; bảng máy đọc được:
  `outputs/metrics/phase3_eda.json`.

### Vấn đề còn lại

- Cảnh báo thiếu native Hadoop/winutils và thông báo dọn JVM của sandbox vẫn
  xuất hiện, nhưng cả ghi/đọc Silver và mọi test đều trả exit code `0`.
- Giá trị `__unknown__` chiếm tỷ trọng lớn trong `service`; phải giữ
  `StringIndexer(handleInvalid="keep")` ở Phase 4 để xử lý cả category mới.
- Correlation đơn biến không xử lý phi tuyến hoặc tương tác; không loại feature
  chỉ dựa vào bảng này.
- Test set chỉ được làm sạch bằng quy tắc đã xác định và chưa dùng để chọn
  feature, transformer, mô hình hoặc threshold.

### Phase tiếp theo

Phase 4 — Xây dựng Spark ML Pipeline: chốt cột số/categorical/cột loại, chia
train/validation có seed cố định, đóng StringIndexer, OneHotEncoder,
VectorAssembler và scaler/model trong một Pipeline duy nhất, rồi kiểm thử schema
đầu ra và chống leakage.

## 2026-09-10 20:27 +07:00 — Phase 4: Xây dựng Spark ML Pipeline

### Việc đã làm

- Chốt hợp đồng feature gồm 3 cột categorical (`proto`, `service`, `state`) và
  39 cột numeric; loại `id`, `attack_cat` và `label` khỏi vector đặc trưng.
- Chia `silver/train` xác định bằng
  `pmod(xxhash64(id, seed), hash_buckets)` với seed `42`, không dùng random split
  theo thứ tự partition:
  - Modeling train: 140.471 dòng.
  - Validation: 34.870 dòng (`19,8870%`).
  - Tổng đúng 175.341 dòng và giao nhau theo `id` bằng 0.
- Giữ official test 82.332 dòng ngoài quy trình fit/transform/evaluate của phase
  này. Test chỉ được đếm để xác nhận artifact vẫn tồn tại và không bị thay đổi.
- Xây dựng một Spark ML `Pipeline` duy nhất gồm:
  - Ba `StringIndexer(handleInvalid="keep")`.
  - `OneHotEncoder(handleInvalid="keep", dropLast=False)`.
  - `VectorAssembler` cho 39 numeric feature.
  - `StandardScaler(withMean=False, withStd=True)` để không làm đặc vector.
  - `VectorAssembler` cuối và `LogisticRegression` smoke 1 iteration.
- Chỉ gọi `fit()` trên modeling train; validation chỉ được đưa qua
  `PipelineModel.transform()`.
- Pipeline tạo vector `features` 199 chiều và prediction schema đầy đủ gồm
  `label`, `rawPrediction`, `probability`, `prediction` trên toàn bộ 34.870 dòng
  validation.
- Lưu modeling split dạng Snappy Parquet và sinh báo cáo kỹ thuật. Không lưu
  smoke model vì đây chưa phải baseline đã đánh giá.
- Bổ sung ba kiểm thử cho hợp đồng loại cột, tính xác định/không giao nhau của
  split và khả năng transform categorical value chưa xuất hiện trong train.

### File đã tạo hoặc thay đổi

- `configs/default.yaml`
- `README.md`
- `src/features.py`
- `tests/test_features.py`
- `docs/FEATURE_PIPELINE.md`
- `docs/NHAT_KY_THUC_HIEN.md`
- Sinh cục bộ, không commit:
  - `data/silver/modeling/train/`.
  - `data/silver/modeling/validation/`.
  - `outputs/metrics/phase4_pipeline.json`.

### Lệnh kiểm thử và kết quả

1. Test chuyên biệt xử lý category chưa từng thấy sau khi hoàn thiện pipeline:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest tests\test_features.py::test_feature_pipeline_keeps_unseen_categories -q
   ```

   Kết quả: exit code `0`, `1 passed in 15.43s`.

2. Chạy pipeline smoke end-to-end:

   ```powershell
   .\.venv\Scripts\python.exe -m src.features
   ```

   Kết quả: exit code `0`, `status=ok`; fit đủ 8 stage trong khoảng 14,967 giây
   trên 140.471 dòng modeling train, transform đủ 34.870 dòng validation và tạo
   vector 199 chiều. Official test không được transform/evaluate.

3. Toàn bộ regression suite sau mọi thay đổi:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả xác minh cuối: exit code `0`, `11 passed in 95.53s`.

### Tiêu chí hoàn thành

- Một pipeline duy nhất đã `fit(train)` và `transform(validation)` thành công.
- Mọi transformer và classifier đều chỉ fit trên modeling train; validation và
  official test không tham gia fit.
- Test schema/vector, split chống leakage và unseen category đều đạt.

### Vấn đề còn lại

- Logistic Regression 1 iteration chỉ là smoke kỹ thuật, chưa phải baseline;
  chưa tính metrics, chọn threshold hoặc lưu model ở Phase 4.
- Spark báo không nạp được `libopenblas.dll` và dùng BLAS Java fallback; pipeline
  vẫn hoàn thành với exit code `0`. Có thể tối ưu native BLAS sau nếu benchmark
  Phase 5 cho thấy cần thiết.
- Sandbox tiếp tục in `ERROR: Access denied` khi dọn tiến trình JVM sau khi
  pytest đã hoàn tất; pytest vẫn trả exit code `0` và đủ 11 test đạt.

### Phase tiếp theo

Phase 5 — Baseline Logistic Regression: huấn luyện baseline tái lập, tính
confusion matrix cùng các chỉ số bắt buộc, khảo sát nhiều threshold chỉ trên
validation, chọn threshold mặc định và ghi thời gian fit/predict cùng cấu hình
Spark.

## 2026-09-10 20:41 +07:00 — Phase 5: Baseline Logistic Regression

### Việc đã làm

- Thêm Logistic Regression baseline không class weight và không tuning để giữ
  đường chuẩn rõ ràng trước Phase 6. Cấu hình cố định: `maxIter=50`,
  `regParam=0`, `elasticNetParam=0`, `tol=1e-6`, có intercept và không
  standardize lần hai vì numeric feature đã được scale trong pipeline.
- Tái sử dụng một pipeline 8 stage của Phase 4; mọi indexer, encoder, scaler và
  classifier chỉ fit trên 140.471 dòng modeling train.
- Transform 34.870 dòng validation và giữ nguyên official test 82.332 dòng;
  test không được transform, evaluate hay dùng để chọn threshold.
- Tính ROC-AUC và PR-AUC bằng Spark `BinaryClassificationEvaluator`.
- Tính confusion matrix, Precision, Recall, F1, Accuracy, FPR, Specificity và
  False Negative Rate tại 17 threshold từ 0,10 đến 0,90. Phần xử lý chạy phân
  tán; driver chỉ nhận 17 hàng aggregate.
- Chọn threshold mặc định `0,55` bằng F1 cao nhất trên validation; khi hòa ưu
  tiên Recall cao hơn, FPR thấp hơn, rồi threshold gần 0,5 hơn.
- Kết quả tại threshold 0,55:
  - TP=23.382, FP=1.840, TN=9.231, FN=417.
  - Precision=`0,927048`, Recall=`0,982478`, F1=`0,953959`.
  - FPR=`0,166200`, Accuracy tham khảo=`0,935274`.
  - ROC-AUC=`0,984068`, PR-AUC=`0,991605`.
- Giải thích Accuracy không đủ: validation có 68,25% Attack nên chiến lược chỉ
  đoán Attack đã đạt khoảng 68,25% accuracy; Accuracy cũng không phân biệt bỏ
  sót tấn công (FN) với cảnh báo nhầm (FP).
- Ghi Spark `4.2.0`, `local[2]`, 4 shuffle partitions, 2 GB driver memory, thời
  gian fit/predict và toàn bộ objective history vào JSON để truy nguyên.
- Chạy baseline hai lần liên tiếp: threshold, confusion matrix và mọi metrics
  giống hệt; chỉ application ID và thời gian thực thi thay đổi.

### File đã tạo hoặc thay đổi

- `configs/default.yaml`
- `README.md`
- `src/evaluate.py`
- `src/train_binary.py`
- `tests/test_model_smoke.py`
- `docs/BASELINE_LOGISTIC_REGRESSION.md`
- `docs/NHAT_KY_THUC_HIEN.md`
- Sinh cục bộ, không commit:
  - `outputs/metrics/phase5_logistic_regression.json`.

### Lệnh kiểm thử và kết quả

1. Test chuyên biệt metrics và threshold:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest tests\test_model_smoke.py -q
   ```

   Kết quả: exit code `0`, `3 passed in 42.54s`.

2. Huấn luyện và đánh giá baseline end-to-end, chạy hai lần:

   ```powershell
   .\.venv\Scripts\python.exe -m src.train_binary
   ```

   Cả hai lần đều exit code `0`, `status=ok` và cho chính xác cùng threshold
   `0,55`, confusion matrix, ROC-AUC/PR-AUC và threshold curve. Artifact cuối có
   thời gian fit `16,561s`, predict `1,670s`.

3. Toàn bộ regression suite sau mọi thay đổi:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả xác minh cuối: exit code `0`, `14 passed in 110.35s`.

### Tiêu chí hoàn thành

- Baseline tái lập được nhờ modeling split seed `42`, tham số và Spark config
  cố định; hai lần chạy thực tế cho metrics giống nhau.
- Đã có đầy đủ Precision, Recall, F1, ROC-AUC, PR-AUC, confusion matrix và FPR;
  Accuracy chỉ dùng tham khảo và có giải thích hạn chế.
- Threshold mặc định `0,55` được chọn hoàn toàn trên validation, không dùng test.

### Vấn đề còn lại

- Optimizer dùng đủ 50 iteration cấu hình; objective tiếp tục giảm nhẹ ở cuối.
  Đây là baseline hợp lệ và tái lập, nhưng Phase 6 cần so sánh regularization
  hữu hạn/class weight thay vì diễn giải hệ số của nghiệm chưa ổn định.
- FPR 16,62% còn cao dù Recall 98,25%; Phase 6 cần đánh giá trade-off này khi so
  sánh Random Forest và chiến lược xử lý mất cân bằng.
- Chưa lưu model hoặc đánh giá official test; hai việc này được cố ý hoãn đến
  Phase 7 sau khi so sánh và khóa lựa chọn ở Phase 6.
- Cảnh báo native Hadoop/BLAS và `ERROR: Access denied` lúc sandbox dọn JVM vẫn
  xuất hiện, nhưng mọi lệnh đều exit code `0`.

### Phase tiếp theo

Phase 6 — Mô hình so sánh và xử lý mất cân bằng: huấn luyện Random Forest, thử
`weightCol` hoặc chiến lược cân bằng hợp lý trong phạm vi nhỏ, so sánh chất
lượng/thời gian/kích thước mô hình với baseline và kiểm tra rõ FP/FN trước khi
chọn mô hình cuối.

## 2026-09-10 20:56 +07:00 — Phase 6: Mô hình so sánh và xử lý mất cân bằng

### Việc đã làm

- Thiết kế tìm kiếm nhỏ, cố định gồm bốn candidate dùng chung modeling split,
  feature pipeline và 17 threshold validation:
  - Logistic Regression không trọng số.
  - Logistic Regression với `weightCol` cân bằng.
  - Random Forest 30 cây, depth 8.
  - Random Forest 50 cây, depth 12.
- Tính trọng số hoàn toàn từ modeling train theo `N/(2*n_class)`:
  - Normal (0): `1,563255`.
  - Attack (1): `0,735127`.
  Tổng trọng số hiệu dụng của hai lớp bằng nhau; validation không tham gia tính
  trọng số.
- Cố định seed Random Forest `42`, `featureSubsetStrategy=sqrt`,
  `subsamplingRate=0,8`, `maxBins=32`, `minInstancesPerNode=2`.
- Fit từng candidate trên 140.471 dòng train, transform 34.870 dòng validation,
  chọn threshold riêng bằng F1 và tính đầy đủ confusion matrix, Precision,
  Recall, F1, FPR, ROC-AUC, PR-AUC.
- Lưu candidate model dưới `models/phase6_candidates/` chỉ để đo kích thước thật;
  đây chưa phải model cuối được phát hành.
- Kết quả so sánh tại threshold validation được chọn:
  - LR gốc @0,55: F1=`0,953959`, Recall=`0,982478`, FPR=`0,166200`,
    FP=1.840, FN=417; fit `18,727s`, size `18.194 bytes`.
  - LR cân bằng @0,40: F1=`0,954219`, Recall=`0,975629`, FPR=`0,148857`,
    FP=1.648, FN=580; fit `12,731s`, size `18.220 bytes`.
  - RF depth 8 @0,65: F1=`0,957270`, Recall=`0,988403`, FPR=`0,164755`,
    FP=1.824, FN=276; fit `9,711s`, size `99.919 bytes`.
  - RF depth 12 @0,60: F1=`0,962225`, Recall=`0,977730`, FPR=`0,117153`,
    FP=1.297, FN=530; fit `25,585s`, size `394.258 bytes`.
- Chọn `random_forest_depth12` tại threshold `0,60` vì F1 validation cao nhất;
  ROC-AUC=`0,990170`, PR-AUC=`0,995342`. So với LR baseline, mô hình giảm 543
  cảnh báo nhầm và tăng 113 tấn công bị bỏ sót trên cùng validation, thể hiện rõ
  trade-off FPR/Recall.
- Xuất JSON đầy đủ và aggregate nhỏ riêng cho dashboard tương lai. Official test
  82.332 dòng vẫn chưa được transform/evaluate.

### File đã tạo hoặc thay đổi

- `configs/default.yaml`
- `README.md`
- `src/train_compare.py`
- `tests/test_model_comparison.py`
- `docs/MODEL_COMPARISON.md`
- `docs/NHAT_KY_THUC_HIEN.md`
- Sinh cục bộ, không commit:
  - `models/phase6_candidates/` — bốn candidate model.
  - `outputs/metrics/phase6_model_comparison.json`.
  - `outputs/dashboard/model_comparison.json`.

### Lệnh kiểm thử và kết quả

1. Test chuyên biệt trọng số lớp và quy tắc chọn candidate:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest tests\test_model_comparison.py -q
   ```

   Lần đầu: `1 failed, 1 passed` do test chưa alias cột Spark
   `sum(class_weight)` nhưng lại truy cập tên `weight_sum`. Đã thêm alias rõ ràng.
   Lần chạy lại: exit code `0`, `2 passed in 27.82s`.

2. So sánh mô hình end-to-end:

   ```powershell
   .\.venv\Scripts\python.exe -m src.train_compare
   ```

   Lần đầu dừng trước khi fit do Spark 4.2 không hỗ trợ keyword `threshold` trong
   constructor `RandomForestClassifier`; threshold của đồ án được áp dụng từ
   probability trong evaluation nên đã bỏ keyword dư. Lần chạy lại: exit code
   `0`, `status=ok`, cả bốn candidate fit/evaluate/save thành công và chọn
   `random_forest_depth12` @ `0,60`.

3. Toàn bộ regression suite sau mọi thay đổi:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả xác minh cuối: exit code `0`, `16 passed in 122.63s`.

### Tiêu chí hoàn thành

- Có bảng so sánh hai Logistic Regression và hai Random Forest trên cùng
  validation, gồm chất lượng, thời gian, kích thước, FP và FN.
- Đã thử chiến lược `weightCol` cân bằng chỉ từ train và ghi rõ tác động.
- Đã chọn Random Forest depth 12 kèm threshold/lý do từ validation, không dùng
  official test.
- Tuning chỉ gồm hai cấu hình Random Forest, phù hợp giới hạn laptop.

### Vấn đề còn lại

- Mô hình được chọn giảm FPR đáng kể nhưng bỏ sót thêm 113 Attack so với LR
  baseline; cần trình bày trade-off này, không chỉ nêu F1 cao hơn.
- Random Forest được chọn lớn hơn LR khoảng 21,7 lần và fit chậm hơn baseline
  khoảng 36,6% trong lần đo Phase 6, dù predict nhanh hơn trong lần chạy này.
- Candidate model chưa được coi là artifact cuối, chưa load lại trong tiến trình
  độc lập và chưa dự đoán official test.
- Spark cảnh báo broadcast task binary lớn tối đa khoảng 2,9 MiB khi huấn luyện
  RF depth 12; job vẫn hoàn thành trên cấu hình 2 GB driver.
- Các cảnh báo native Hadoop/BLAS và `ERROR: Access denied` lúc sandbox dọn JVM
  vẫn không ảnh hưởng exit code.

### Phase tiếp theo

Phase 7 — Đánh giá cuối, lưu model và batch inference: khóa Random Forest depth
12 cùng threshold `0,60`, đánh giá official test đúng một lần, lưu model cuối,
nạp lại trong tiến trình độc lập và xuất prediction có metadata mà không huấn
luyện lại.

## 2026-09-10 21:14 +07:00 — Phase 7: Đánh giá cuối, lưu model và batch inference

### Việc đã làm

- Khóa đúng `random_forest_depth12` và threshold `0,60` đã chọn ở Phase 6. Không
  refit bằng validation để giữ nguyên candidate/xác suất đã được thẩm định.
- Nạp candidate Phase 6 từ đĩa, xác minh classifier có 50 cây rồi lưu thành
  `models/final/pipeline/` với 8 stage. Artifact có 64 file, 394.310 bytes và
  SHA-256 `750231e41ffc674d31762f31082857ee49ad2f33a540ad42f0f0406074f93a2f`.
- Ghi `model_metadata.json` chứa model hash, source-report hash, tham số, hợp đồng
  nhãn, threshold, validation metrics và trạng thái audit official test.
- Trong tiến trình Python/Spark mới, nạp model cuối và smoke batch inference trên
  validation mà không huấn luyện/đánh giá. Đã ghi, đọc lại đủ 34.870 dòng ở cả
  Parquet và CSV.
- Chỉ sau khi smoke thành công mới mở official test. Guard được tạo trước lần
  chạy và hoàn tất với `evaluation_count=1`, run ID `phase7-43839df4de0a`.
- Đánh giá 82.332 dòng official test đúng một lần, chỉ tại threshold validation
  đã khóa `0,60`; không quét threshold hay chọn lại model trên test.
- Kết quả official test:
  - Phân phối: Normal=37.000, Attack=45.332 (`55,0600%` Attack).
  - TP=44.766, FP=10.789, TN=26.211, FN=566.
  - Precision=`0,805796`, Recall=`0,987514`, F1=`0,887448`.
  - FPR=`0,291595`, Accuracy tham khảo=`0,862083`.
  - ROC-AUC=`0,978606`, PR-AUC=`0,983752`.
- So với validation tại cùng threshold, F1 test giảm `0,074776` và FPR tăng
  `0,174442`, dù Recall tăng nhẹ. Giữ nguyên kết quả như bằng chứng khác biệt
  phân phối/khả năng tổng quát hóa, không tối ưu lại theo test.
- Batch output gồm run ID, thời điểm, model name/hash, threshold, trường mô tả,
  nhãn thật, xác suất Attack, prediction 0,5 của Spark, prediction threshold đã
  khóa, tên lớp và cờ đúng/sai.
- Ghi và đọc lại đủ 82.332 prediction:
  - Parquet khoảng 944.519 bytes.
  - CSV khoảng 18.431.367 bytes.
- Đo thời gian tiến trình test: load model `9,759s`, transform/count `9,375s`,
  evaluate `8,640s`, ghi/đọc lại output `3,320s`.
- Thử gọi official-test command lần hai; guard từ chối trước khi khởi tạo Spark,
  xác nhận không đánh giá lặp lại.

### File đã tạo hoặc thay đổi

- `configs/default.yaml`
- `README.md`
- `src/finalize_model.py`
- `src/predict_batch.py`
- `tests/test_batch_prediction.py`
- `docs/FINAL_TEST_REPORT.md`
- `docs/NHAT_KY_THUC_HIEN.md`
- Sinh cục bộ, không commit:
  - `models/final/pipeline/`.
  - `models/final/model_metadata.json`.
  - `outputs/predictions/phase7_smoke_parquet/` và `phase7_smoke_csv/`.
  - `outputs/predictions/phase7_smoke_metadata.json`.
  - `outputs/predictions/final_test_parquet/` và `final_test_csv/`.
  - `outputs/metrics/phase7_final_test.json`.
  - `outputs/metrics/phase7_official_test_guard.json`.

### Lệnh kiểm thử và kết quả

1. Test checksum và output theo threshold khóa:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest tests\test_batch_prediction.py -q
   ```

   Hai lần đầu phát hiện lỗi fixture/cấu hình trước khi tạo model cuối: khối YAML
   chèn sai vị trí, sau đó DDL test dùng kiểu `vector` không được Spark SQL hỗ
   trợ. Đã sửa YAML và cho Spark suy luận `VectorUDT`. Kết quả cuối: exit code
   `0`, `2 passed in 26.70s`.

2. Khóa/lưu model cuối (không đọc official test):

   ```powershell
   .\.venv\Scripts\python.exe -m src.finalize_model
   ```

   Kết quả: exit code `0`, `status=locked`; candidate được load và final pipeline
   được lưu với hash/metadata đầy đủ.

3. Load model trong tiến trình khác và smoke inference không evaluate:

   ```powershell
   .\.venv\Scripts\python.exe -m src.predict_batch --input data\silver\modeling\validation --output-name phase7_smoke --no-evaluate
   ```

   Kết quả: exit code `0`, `status=ok`; model load không training, Parquet/CSV
   đều đọc lại đúng 34.870 dòng.

4. Official test evaluation duy nhất:

   ```powershell
   .\.venv\Scripts\python.exe -m src.predict_batch
   ```

   Kết quả: exit code `0`, `status=ok`; 82.332 prediction và metrics cuối đã
   sinh. Lần gọi kế tiếp exit code `1` với thông báo guard đã started/completed,
   trước khi Spark khởi tạo; đây là hành vi bảo vệ mong đợi.

5. Toàn bộ regression suite sau mọi thay đổi:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả xác minh cuối: exit code `0`, `18 passed in 115.80s`.

### Tiêu chí hoàn thành

- PipelineModel cuối đã lưu và nạp thành công ở các tiến trình độc lập.
- Batch inference chạy không huấn luyện lại, áp dụng threshold validation đã
  khóa và xuất đủ Parquet/CSV có metadata.
- Official test được đánh giá đúng một lần; metrics và predictions cùng truy về
  run ID/model SHA-256.

### Vấn đề còn lại

- FPR official test `29,16%` cao hơn đáng kể validation `11,72%`; đây là hạn chế
  tổng quát hóa cần nêu rõ trong báo cáo và demo, không được che bằng Accuracy
  hoặc chỉnh threshold sau khi đã xem test.
- CSV lớn hơn Parquet khoảng 19,5 lần; đây là dữ liệu đầu vào hữu ích cho
  benchmark định dạng ở Phase 8 nhưng chưa được coi là phép đo benchmark vì nội
  dung CSV/Parquet prediction không hoàn toàn tương đương dữ liệu nguồn.
- Cảnh báo broadcast binary khoảng 1,9 MiB và cảnh báo native Hadoop/BLAS vẫn
  xuất hiện; batch job hoàn tất với exit code `0` trên driver 2 GB.

### Phase tiếp theo

Phase 8 — Chứng minh giá trị Apache Spark: lưu logical/physical plan, ghi bằng
chứng Job/Stage/Task/partition/shuffle và chạy benchmark có kiểm soát cho
CSV/Parquet, cache/no-cache, cùng một vài mức partition trên cấu hình máy hiện
tại.

## 2026-09-10 21:31 +07:00 — Phase 8: Chứng minh giá trị của Apache Spark

### Việc đã làm

- Xây dựng truy vấn đại diện chung:
  `filter(label) -> groupBy(label, proto) -> count/sum bytes -> orderBy`.
  Truy vấn chỉ collect 136 hàng aggregate, kiểm kê đủ 175.341 dòng nguồn.
- Lưu logical plan, optimized logical plan và physical plan trước/sau Adaptive
  Query Execution. Plan chứa 4 lần xuất hiện `Exchange` trên cả initial/final
  adaptive plan, thể hiện shuffle do `groupBy` và `orderBy`.
- Dùng Spark status tracker với job group riêng để ghi bằng chứng một action:
  - 4 Job đều `SUCCEEDED`.
  - 8 stage ID dưới AQE, tổng 20 task được khai báo.
  - 7 task thuộc 4 stage thực sự hoàn tất, 0 task thất bại; các stage còn lại là
    phương án adaptive bị skip.
- Ghi cấu hình benchmark: Windows 11, Intel x64, 12 logical CPU, khoảng 16 GB
  RAM; Python `3.12.13`, Java `21.0.12.1`, Spark `4.2.0`, `local[2]`, một local
  executor process/tối đa 2 task đồng thời, driver 2 GB và 4 shuffle partitions.
- Thiết kế protocol dùng `time.perf_counter`, một warmup, ba lần đo; đảo thứ tự
  CSV/Parquet xen kẽ và chỉ so sánh khi hash aggregate giống nhau.
- Benchmark CSV/Parquet trên cùng 175.341 dòng Bronze và cùng schema/truy vấn:
  - CSV 32.293.018 bytes, median `0,3288s`.
  - Parquet 13.174.898 bytes, median `0,1981s`.
  - Parquet nhỏ hơn khoảng 2,45 lần và nhanh hơn median `1,66x`; kết quả aggregate
    có cùng SHA-256.
- Benchmark cache khi tái sử dụng cùng Parquet DataFrame:
  - Không cache median/action `0,2167s`.
  - Có cache median/action `0,1867s`, nhanh hơn `1,16x` ở reuse action.
  - Materialize cache mất `1,1041s`; tổng ba reuse là `0,6264s` không cache so
    với `1,7206s` có tính materialize. Ước tính khoảng 37 reuse mới hòa vốn nếu
    median giữ ổn định, nên không kết luận cache mặc nhiên nhanh hơn.
- Benchmark partition sau khi repartition/cache/materialize cùng dữ liệu:
  - 1 partition: aggregate median `0,0664s`, materialize `1,2986s`.
  - 2 partitions: aggregate median `0,1407s`, materialize `0,8043s`.
  - 4 partitions: aggregate median `0,1266s`, materialize `0,7462s`.
  - 8 partitions: aggregate median `0,1781s`, materialize `0,8007s`.
  Aggregate nhỏ nhanh nhất với 1 partition; materialize nhanh nhất ở 4. Đây chỉ
  là kết quả của truy vấn/dữ liệu/local[2] hiện tại, không phải quy tắc chung.
- Viết sơ đồ và giải thích quan hệ Partition → Task → Core → Stage → Job, lazy
  transformation/action, shuffle/Exchange, cache và lý do dùng Spark.
- Nêu trung thực rằng subset 175 nghìn dòng vẫn có thể xử lý bằng Pandas; giá trị
  của Spark nằm ở pipeline phân tán thống nhất và khả năng mở rộng tới bộ đầy đủ
  khoảng 2,54 triệu dòng/cluster, không phải tuyên bố Spark luôn nhanh hơn.

### File đã tạo hoặc thay đổi

- `configs/default.yaml`
- `README.md`
- `src/benchmark.py`
- `tests/test_benchmark.py`
- `docs/SPARK_EXECUTION_AND_BENCHMARK.md`
- `docs/NHAT_KY_THUC_HIEN.md`
- Sinh cục bộ, không commit:
  - `outputs/benchmarks/phase8_benchmark.json`.
  - `outputs/benchmarks/representative_query_plan.txt`.

### Lệnh kiểm thử và kết quả

1. Test aggregate, signature, timing summary và dấu vết Exchange:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest tests\test_benchmark.py -q
   ```

   Kết quả: exit code `0`, `3 passed in 27.86s`.

2. Chạy benchmark end-to-end:

   ```powershell
   .\.venv\Scripts\python.exe -m src.benchmark
   ```

   Kết quả: exit code `0`, `status=ok`; toàn bộ so sánh có kết quả aggregate
   giống nhau, 0 task thất bại và các artifact plan/JSON/báo cáo được sinh.

3. Toàn bộ regression suite sau mọi thay đổi:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả xác minh cuối sau mọi cập nhật: exit code `0`,
   `21 passed in 111.22s`.

### Tiêu chí hoàn thành

- Có bảng benchmark tái lập cho CSV/Parquet, cache/no-cache và 1/2/4/8
  partitions cùng protocol/cấu hình máy.
- Có logical/optimized/physical plan và bằng chứng thực tế Job/Stage/Task từ
  status tracker.
- Có sơ đồ/giải thích Partition → Task → Core → Stage → Job, shuffle và lý do
  Spark phù hợp hơn cho hướng mở rộng của đồ án.

### Vấn đề còn lại

- Benchmark local với ba measured runs vẫn chịu ảnh hưởng OS filesystem cache,
  JVM JIT và nhiễu scheduler; không được ngoại suy thành kết quả cluster.
- Một action `collect()` có bốn Spark Job dưới AQE; một số stage ID có
  `numCompletedTasks=0` vì bị adaptive plan skip. Báo cáo phân biệt task khai
  báo và task hoàn tất để tránh cộng sai.
- Cache không hoàn vốn ở ba lần dùng và 1 partition thắng aggregate nhỏ; dashboard
  và báo cáo không được diễn giải hai kết quả này như khuyến nghị phổ quát.
- Cảnh báo native Hadoop và `ERROR: Access denied` lúc sandbox dọn JVM vẫn xuất
  hiện sau exit code `0`.

### Phase tiếp theo

Phase 9 — Dashboard trình bày: xây Streamlit dashboard chỉ đọc aggregate/metrics
và prediction đã giới hạn, gồm tổng quan dữ liệu, kết quả mô hình/confusion
matrix/ROC-PR, danh sách cảnh báo và tùy chọn dự đoán một record mà không kéo
toàn bộ dữ liệu về Pandas.

## 2026-09-10 21:56 +07:00 — Phase 9: Dashboard trình bày

### Việc đã làm

- Cài và ghim `streamlit==1.63.0`, `altair==6.2.2`; ghim
  `pandas==2.3.3` thay vì bản 3.x do PySpark 4.2 cảnh báo Pandas 3 chưa được hỗ
  trợ. Giữ toàn bộ dependency trong `.venv` của dự án.
- Tạo exporter Phase 9 đọc prediction Parquet bằng Spark và chỉ đưa về driver
  dữ liệu đã giới hạn/aggregate:
  - Kiểm kê đủ 82.332 prediction và đối chiếu với metrics Phase 7.
  - Xuất 200 flow được dự đoán Attack có xác suất cao nhất.
  - Xuất tối đa 50 false positive và 50 false negative.
  - Aggregate top-15 theo protocol, service và attack category.
  - Không gọi `toPandas()` và không collect toàn bộ prediction.
- Tạo manifest 633 bytes và bundle cảnh báo khoảng 104 KB; manifest đánh dấu
  `offline_ready=true` và `large_dataset_loaded_by_dashboard=false`.
- Xây Streamlit dashboard chạy offline, không import/khởi tạo Spark, gồm bốn
  trang:
  - Tổng quan: 175.341 flow train, tỷ lệ Attack, protocol/service phổ biến,
    phân phối nhãn và nhóm tấn công.
  - Mô hình: metrics official test, confusion matrix, bảng candidate, threshold
    trade-off cùng ROC/PR operating points validation. ROC-AUC và PR-AUC chính
    xác lấy từ Spark evaluator; đường ROC/PR chỉ nối 17 threshold đã đo.
  - Cảnh báo: lọc mẫu theo probability/protocol/service và xem false
    positive/false negative.
  - Spark benchmark: CSV/Parquet, cache, partition và bằng chứng execution từ
    Phase 8.
- Viết cấu hình Streamlit headless/local và tài liệu khởi động một lệnh. Giao
  diện dùng CSS/emoji nội bộ, không tải font hay asset từ Internet.
- Chạy server thật tại `http://127.0.0.1:8501`, health endpoint trả `ok`; kiểm
  tra bằng trình duyệt rằng trang Tổng quan và Mô hình render số liệu/biểu đồ và
  điều hướng được. Lần mở đầu phát hiện `ModuleNotFoundError` chỉ xuất hiện với
  `streamlit run`; đã thêm project root vào `sys.path` và xác nhận chạy lại
  thành công.

### File đã tạo hoặc thay đổi

- `requirements.txt`
- `configs/default.yaml`
- `README.md`
- `.streamlit/config.toml`
- `src/export_dashboard_data.py`
- `dashboard/__init__.py`
- `dashboard/data_access.py`
- `dashboard/app.py`
- `tests/test_dashboard.py`
- `docs/DASHBOARD_GUIDE.md`
- `docs/NHAT_KY_THUC_HIEN.md`
- Sinh cục bộ, không commit:
  - `outputs/dashboard/manifest.json`.
  - `outputs/dashboard/alerts_sample.json`.

### Lệnh kiểm thử và kết quả

1. Sinh bundle dashboard từ prediction Phase 7:

   ```powershell
   .\.venv\Scripts\python.exe -m src.export_dashboard_data
   ```

   Kết quả: exit code `0`, `status=ready`; kiểm kê 82.332 prediction, xuất đúng
   giới hạn 200 cảnh báo, 50 false positive và 50 false negative.

2. Test data contract, giới hạn driver và render cả bốn trang:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest tests\test_dashboard.py -q
   ```

   Kết quả cuối: exit code `0`, `3 passed in 1.69s`.

3. Smoke test server thật:

   ```powershell
   .\.venv\Scripts\streamlit.exe run dashboard\app.py --server.headless true --server.address 127.0.0.1 --server.port 8501
   ```

   Kết quả: server lắng nghe ở `127.0.0.1:8501`, health endpoint trả `ok`,
   trình duyệt render thành công sau khi sửa đường import.

4. Toàn bộ regression suite sau mọi thay đổi:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   ```

   Kết quả: exit code `0`, `24 passed in 109.08s`. Dòng `ERROR: Access denied`
   vẫn xuất hiện sau khi pytest đã kết thúc thành công do sandbox dọn JVM, giống
   các phase Spark trước và không làm thay đổi exit code.

### Tiêu chí hoàn thành

- Dashboard khởi động bằng lệnh rõ ràng và đã được xác nhận trên server/trình
  duyệt thật.
- Dashboard không đọc dataset lớn hoặc kéo toàn bộ dữ liệu về Pandas; chỉ đọc
  JSON đã aggregate/giới hạn.
- Bốn trang chính trình bày đủ overview, model/confusion matrix/ROC-PR, cảnh
  báo mẫu và benchmark; bundle chạy hoàn toàn offline.

### Vấn đề còn lại

- Danh sách cảnh báo là mẫu top 200, không phải toàn bộ 66.981 prediction Attack;
  đây là giới hạn có chủ đích để giữ dashboard nhẹ và an toàn bộ nhớ.
- Form dự đoán một record là hạng mục tùy chọn nên không triển khai: khởi tạo
  Spark cho từng tương tác làm mất lợi thế demo offline. Batch inference vẫn là
  đường suy luận chính thức; sau đó có thể export lại bundle.
- Dashboard là lớp trình bày kết quả nghiên cứu, không phải IDS production,
  không đọc traffic thời gian thực và không được dùng để tuyên bố khả năng bảo
  vệ mạng trực tiếp.

### Phase tiếp theo

Phase 10 — Structured Streaming tùy chọn: phát lại record bằng file
micro-batch, dùng `readStream` với schema cố định, nạp PipelineModel cuối mà
không train lại, ghi prediction/checkpoint và kiểm tra restart. Đây là phần mở
rộng; toàn bộ phạm vi batch bắt buộc và dashboard đã hoàn thành.

## 2026-09-10 22:25 +07:00 — Phase 10: Structured Streaming tùy chọn

### Việc đã làm

- Xây dựng src/streaming_predict.py cho mô phỏng file-source Parquet với
  UNSW_NB15_SCHEMA tường minh, không infer schema.
- Nạp và kiểm SHA-256 của PipelineModel Phase 7; code streaming chỉ load model,
  transform và áp dụng threshold validation 0,60, không gọi fit hoặc train lại.
- Thiết kế maxFilesPerTrigger=1 và AvailableNow: backlog thành nhiều
  micro-batch, sau đó query tự dừng, phù hợp demo offline.
- Công bố file sample bằng staging rồi atomic move; mỗi file gồm 20 record.
- Trong foreachBatch, ghi prediction và batch summary theo stream_batch_id.
  Output batch retry overwrite đúng thư mục batch thay vì append trùng.
- Bổ sung checkpoint manager FileSystemBasedCheckpointFileManager để Windows
  dùng pure-Java local filesystem thay vì FileContext gọi winutils.exe.
- Chạy demo end-to-end hai Spark application:
  - Run đầu xử lý 2 file, batch 0 và 1, đủ 40 prediction.
  - Sau khi dừng Spark, thêm file thứ ba rồi restart cùng checkpoint.
  - Restart chỉ xử lý batch 2; tổng 60 prediction/60 ID phân biệt/3 summary.
  - Persistent query ID giữ nguyên, query run ID đổi; sáu restart checks đều true.

### File đã tạo hoặc thay đổi

- configs/default.yaml
- .gitignore
- src/predict_batch.py
- src/streaming_predict.py
- tests/test_streaming.py
- README.md
- docs/STREAMING_GUIDE.md
- data/streaming_input/.gitkeep
- outputs/streaming/.gitkeep
- Sinh cục bộ, không commit:
  - outputs/metrics/phase10_streaming.json.
  - data/streaming_input/phase10_final/.
  - outputs/streaming/phase10_final/ gồm checkpoint, prediction và summary.

### Lệnh kiểm thử và kết quả

1. Kiểm thử schema, aggregate alert và path separation:

~~~powershell
.\.venv\Scripts\python.exe -m pytest tests\test_streaming.py -q
~~~

Kết quả: exit code 0, 3 passed in 20.95s.

2. Demo checkpoint/restart thực tế:

~~~powershell
.\.venv\Scripts\python.exe -m src.streaming_predict --run-name phase10_final
~~~

Kết quả: exit code 0, status ok; 3 micro-batch lần lượt 20 dòng, run đầu
10,018s, restart 4,399s, chỉ file mới được xử lý sau restart.

3. Regression gồm streaming và batch output contract:

~~~powershell
.\.venv\Scripts\python.exe -m pytest tests\test_streaming.py tests\test_batch_prediction.py -q
~~~

Kết quả: exit code 0, 5 passed in 28.32s.

4. Toàn bộ regression suite sau Phase 10:

~~~powershell
.\.venv\Scripts\python.exe -m pytest -q
~~~

Kết quả: exit code 0, 27 passed in 110.60s. Dòng ERROR: Access denied xuất hiện
sau exit code 0 khi sandbox dọn JVM, không làm thay đổi kết quả test.

### Tiêu chí hoàn thành

- File mới tạo prediction mới qua readStream với schema cố định.
- PipelineModel được nạp lại, không train lại trong stream.
- Checkpoint được dùng qua SparkSession khác và không xử lý lại hai file cũ.
- Báo cáo gọi rõ đây là mô phỏng streaming file-source, không phải live packet capture.

### Vấn đề còn lại

- foreachBatch có at-least-once semantics; output batch_id giảm duplicate khi
  retry nhưng prediction và summary chưa là giao dịch distributed production.
- Demo là local AvailableNow, không đo latency liên tục, Kafka, nhiều executor
  hoặc sink giao dịch.
- Cảnh báo Hadoop native/large task binary còn xuất hiện trên Windows local,
  nhưng query/demo kết thúc status ok.

### Phase tiếp theo

Phase 11 — Hoàn thiện báo cáo kỹ thuật, sơ đồ kiến trúc/execution, kịch bản demo
5–8 phút, checklist chạy lại và phương án trình bày offline.

## 2026-09-10 22:40 +07:00 — Phase 11: Báo cáo và chuẩn bị thuyết trình

### Việc đã làm

- Viết docs/BAO_CAO_KY_THUAT.md theo artifact thực tế: phạm vi, nguồn dữ liệu,
  kiến trúc/lineage, kiểm soát leakage, EDA, model selection, official test,
  Spark execution, dashboard/streaming, giới hạn và lệnh tái lập.
- Đối chiếu các metrics chính trong báo cáo với outputs/metrics/phase7_final_test.json:
  Recall 0,987514 và FPR 0,291595 được lấy từ output code, không chép số thủ công.
- Bổ sung sơ đồ ASCII cho pipeline dữ liệu/model và execution partition -> task
  -> Exchange -> stage/action.
- Viết docs/KICH_BAN_DEMO.md cho 5–8 phút: mốc thời gian, câu nói chính, bằng
  chứng cần mở, câu trả lời ngắn, checklist và phương án dự phòng offline.
- Cập nhật README để trỏ người dùng đến báo cáo/kịch bản cuối.
- Thêm test tài liệu để phát hiện report/demo thiếu artifact nguồn, số liệu test,
  giới hạn hoặc runbook.

### File đã tạo hoặc thay đổi

- README.md
- docs/BAO_CAO_KY_THUAT.md
- docs/KICH_BAN_DEMO.md
- docs/NHAT_KY_THUC_HIEN.md
- tests/test_phase11_documents.py

### Lệnh kiểm thử và kết quả

1. Kiểm thử report/demo đối chiếu official-test metrics:

~~~powershell
.\.venv\Scripts\python.exe -m pytest tests\test_phase11_documents.py -q
~~~

Kết quả cuối: exit code 0, 2 passed in 0.12s. Lần đầu test phát hiện khác biệt
trình bày dấu phẩy thập phân Việt Nam với JSON dấu chấm; test được chuẩn hóa định
dạng trước khi đối chiếu nên vẫn kiểm tra đúng trị số nguồn.

2. Toàn bộ regression suite sau mọi artifact Phase 10–11:

~~~powershell
.\.venv\Scripts\python.exe -m pytest -q
~~~

Kết quả: exit code 0, 29 passed in 127.17s. Dòng ERROR: Access denied xuất hiện
sau exit code 0 khi sandbox dọn JVM; không có test nào fail.

### Tiêu chí hoàn thành

- README, báo cáo kỹ thuật và kịch bản demo liên kết rõ ràng.
- Số liệu quan trọng trong báo cáo truy ngược được tới metrics JSON/code.
- Có kiến trúc/execution flow, hạn chế và phương án demo dự phòng offline.

### Vấn đề còn lại

- Nếu giảng viên yêu cầu định dạng Word/PDF hoặc template trích dẫn cụ thể, cần
  chuyển đổi nội dung báo cáo theo mẫu đó; hiện artifact chính là Markdown.

### Trạng thái kế hoạch

Toàn bộ 12/12 phase (0–11) đã có artifact và tiêu chí kiểm chứng. Không còn
phase kế tiếp trong kế hoạch nguồn.
