# UNSW-NB15 Network Intrusion Detection with Apache Spark

Đồ án xây dựng pipeline phát hiện xâm nhập mạng nhị phân `Normal`/`Attack`
bằng PySpark DataFrame, Spark SQL và Spark MLlib. Tài liệu phạm vi và các phase
nằm tại `KE_HOACH_PHAT_HIEN_XAM_NHAP_SPARK.md`.

## Trạng thái

Phase 1–10 đã hoàn thành: môi trường Spark local, Bronze/Silver Parquet, schema
tường minh, validation, làm sạch, EDA bằng Spark SQL, Spark ML Pipeline chống
leakage, Logistic Regression baseline, so sánh mô hình và batch inference bằng
model cuối đã khóa, benchmark/execution evidence và dashboard trình bày offline.
Phần mở rộng Structured Streaming file-source cũng đã được kiểm chứng với
checkpoint/restart. Phase tiếp theo là báo cáo và chuẩn bị thuyết trình.

## Yêu cầu môi trường

- Windows PowerShell.
- Java 17 trở lên và lệnh `java` có trên `PATH`.
- `uv` để cài Python và dependency vào `.venv`.
- Tối thiểu khoảng 3 GB RAM trống cho cấu hình local mặc định.

Apache Spark 4.2 hỗ trợ Python 3.10 trở lên và yêu cầu Java 17 trở lên. Dự án
dùng Python 3.12 để tránh phụ thuộc vào Python cài toàn hệ thống.

## Cài đặt

Chạy từ thư mục gốc của dự án:

```powershell
.\scripts\bootstrap.ps1
```

Không cần cài Spark riêng: gói PySpark từ PyPI cung cấp Spark local và
`spark-submit.cmd` trong `.venv\Scripts`. Nếu Java trên máy mới hơn phiên bản
Spark thực tế hỗ trợ, script bootstrap tải JDK 21 Eclipse Temurin portable vào
`.jdk\jdk-*`. Mã khởi tạo tự động ưu tiên JDK này mà không thay đổi cấu hình Java
toàn hệ thống. Trên Windows, bootstrap còn tải JAR filesystem Java thuần từ
Maven Central để Spark ghi Parquet mà không cần chạy `winutils.exe` không chính
thức; checksum của JAR được kiểm tra trước khi sử dụng.

## Kiểm tra nhanh

```powershell
.\.venv\Scripts\python.exe -m src.spark_session
.\.venv\Scripts\python.exe -m pytest -q
```

Smoke test in JSON phải báo `status: ok`, `input_rows: 3` và hai nhóm
`Normal`/`Attack`. Để giữ tiến trình mở 30 giây và xem Spark UI:

```powershell
.\.venv\Scripts\python.exe -m src.spark_session --hold-seconds 30
```

Mở địa chỉ được in tại `spark_ui_url` (thường là
`http://127.0.0.1:4040`). UI chỉ tồn tại khi SparkSession còn chạy.

Để gọi `spark-submit` trực tiếp trong PowerShell hiện tại:

```powershell
. .\scripts\project_env.ps1
spark-submit --version
```

## Dữ liệu và Phase 2

Nguồn chuẩn là trang UNSW-NB15 của UNSW. Do liên kết SharePoint có thể yêu cầu
đăng nhập, dự án cung cấp mirror đã khóa SHA-256 và ghi rõ provenance trong
`docs/DATASET_SOURCE.md`.

```powershell
.\.venv\Scripts\python.exe -m src.download_data
.\.venv\Scripts\python.exe -m src.ingest
```

Lệnh ingest kiểm tra header, checksum, số dòng, null, trùng lặp, phân phối nhãn
và label không hợp lệ; sau đó ghi và đọc lại:

- `data/bronze/train/`: 175.341 dòng.
- `data/bronze/test/`: 82.332 dòng.
- `data/samples/unsw_nb15_sample_csv/`: 200 dòng cố định, mỗi nhãn 100 dòng.
- `outputs/metrics/phase2_validation.json`: báo cáo máy đọc được.
- `docs/DATA_VALIDATION.md`: báo cáo tóm tắt.

CSV gốc và Bronze Parquet bị loại khỏi commit; sample nhỏ được phép lưu cùng mã
nguồn để test chạy không cần tải toàn bộ dữ liệu.

## Làm sạch và EDA — Phase 3

```powershell
.\.venv\Scripts\python.exe -m src.preprocess
.\.venv\Scripts\python.exe -m src.eda
```

Cleaning giữ hợp đồng 45 cột, chuẩn hóa categorical, xử lý null/NaN/±∞/số âm,
loại target sai và ghi Silver Parquet. EDA chỉ dùng `silver/train`, chạy sáu
truy vấn Spark SQL và chỉ collect các bảng aggregate có giới hạn.

- `data/silver/train/` và `data/silver/test/`: dữ liệu sạch tái sử dụng được.
- `outputs/metrics/phase3_cleaning.json`: audit trước/sau cleaning.
- `outputs/metrics/phase3_eda.json`: truy vấn SQL và toàn bộ aggregate nhỏ.
- `outputs/dashboard/eda_summary.json`: dữ liệu tổng hợp cho dashboard sau này.
- `outputs/figures/*.svg`: bốn biểu đồ EDA.
- `docs/EDA_REPORT.md`: diễn giải phân phối lớp và kết quả EDA.

## Spark ML Pipeline — Phase 4

```powershell
.\.venv\Scripts\python.exe -m src.features
```

Lệnh chia `silver/train` thành modeling train/validation bằng hash xác định theo
`id` và seed, giữ nguyên official test. Một pipeline duy nhất gồm
`StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `StandardScaler` và
Logistic Regression smoke được fit chỉ trên modeling train rồi transform
validation.

- `data/silver/modeling/train/`: 140.471 dòng.
- `data/silver/modeling/validation/`: 34.870 dòng.
- Vector `features`: 199 chiều; không chứa `id`, `attack_cat` hoặc `label`.
- `outputs/metrics/phase4_pipeline.json`: hợp đồng feature, split và kết quả
  smoke máy đọc được.
- `docs/FEATURE_PIPELINE.md`: tài liệu pipeline và nguyên tắc chống leakage.

Logistic Regression 1 iteration ở phase này chỉ là smoke kỹ thuật, chưa phải
baseline hoặc đánh giá mô hình. Metrics và threshold được thực hiện ở Phase 5.

## Logistic Regression baseline — Phase 5

```powershell
.\.venv\Scripts\python.exe -m src.train_binary
```

Baseline fit toàn bộ feature pipeline và Logistic Regression chỉ trên modeling
train, sau đó tính metrics và 17 threshold trên validation. Threshold mặc định
được chọn bằng F1 cao nhất với tie-break ưu tiên Recall rồi FPR thấp hơn.
Official test không được transform hoặc evaluate.

Kết quả validation ở threshold `0,55`: Precision `0,927048`, Recall `0,982478`,
F1 `0,953959`, FPR `0,166200`, ROC-AUC `0,984068` và PR-AUC `0,991605`.
Confusion matrix cùng toàn bộ threshold curve nằm trong:

- `outputs/metrics/phase5_logistic_regression.json`.
- `docs/BASELINE_LOGISTIC_REGRESSION.md`.

Accuracy chỉ được báo để tham khảo vì validation có `68,25%` Attack; Recall,
FPR, PR-AUC và confusion matrix mới cho biết rõ tấn công bị bỏ sót và cảnh báo
nhầm.

## So sánh mô hình — Phase 6

```powershell
.\.venv\Scripts\python.exe -m src.train_compare
```

Thí nghiệm có giới hạn so sánh Logistic Regression gốc, Logistic Regression
dùng `weightCol` cân bằng tính chỉ từ train và hai cấu hình Random Forest. Mỗi
candidate dùng cùng split/pipeline, được chọn threshold riêng trên validation và
lưu tạm dưới `models/phase6_candidates/` để đo kích thước thực.

Random Forest 50 cây, depth 12 được chọn tại threshold `0,60`: Precision
`0,947203`, Recall `0,977730`, F1 `0,962225`, FPR `0,117153`, ROC-AUC
`0,990170` và PR-AUC `0,995342`. So với baseline, candidate này giảm 543 cảnh
báo nhầm nhưng bỏ sót thêm 113 tấn công trên validation.

- `outputs/metrics/phase6_model_comparison.json`: kết quả đầy đủ và threshold
  curve của bốn candidate.
- `outputs/dashboard/model_comparison.json`: aggregate nhỏ cho dashboard.
- `docs/MODEL_COMPARISON.md`: bảng so sánh và lý do chọn mô hình.

Official test vẫn chưa được sử dụng. Các model Phase 6 chỉ là candidate; model
cuối sẽ được khóa, lưu/nạp và đánh giá test đúng một lần ở Phase 7.

## Model cuối và batch inference — Phase 7

Khóa candidate đã thắng mà không refit bằng validation:

```powershell
.\.venv\Scripts\python.exe -m src.finalize_model
```

Batch inference trên dữ liệu Parquet mới, không huấn luyện và không đánh giá
nhãn:

```powershell
.\.venv\Scripts\python.exe -m src.predict_batch --input data\silver\modeling\validation --output-name demo_batch --no-evaluate
```

Trong một workspace sạch chưa có kết quả Phase 7, lệnh dưới đây đánh giá
official test đúng một lần:

```powershell
.\.venv\Scripts\python.exe -m src.predict_batch
```

Workspace hiện tại đã thực hiện lần đánh giá đó; guard
`outputs/metrics/phase7_official_test_guard.json` sẽ từ chối chạy lại để tránh
vô tình điều chỉnh theo test.

Random Forest cuối tại threshold validation `0,60` đạt trên official test:
Precision `0,805796`, Recall `0,987514`, F1 `0,887448`, FPR `0,291595`,
ROC-AUC `0,978606` và PR-AUC `0,983752`. FPR cao hơn validation được giữ nguyên
như kết quả tổng quát hóa, không dùng để chọn lại threshold.

- `models/final/pipeline/`: PipelineModel cuối.
- `models/final/model_metadata.json`: cấu hình khóa, checksum và audit test.
- `outputs/predictions/final_test_parquet/` và `final_test_csv/`: 82.332 dự đoán
  có probability, nhãn, threshold, run ID và model checksum.
- `outputs/metrics/phase7_final_test.json`: metrics/metadata máy đọc được.
- `docs/FINAL_TEST_REPORT.md`: báo cáo đánh giá cuối.

## Spark execution và benchmark — Phase 8

```powershell
.\.venv\Scripts\python.exe -m src.benchmark
```

Benchmark dùng cùng truy vấn exact-integer trên 175.341 dòng, một warmup và ba
lần đo; thứ tự CSV/Parquet được đảo xen kẽ. Trên máy Windows 11, 12 logical CPU,
16 GB RAM nhưng Spark chạy `local[2]`/driver 2 GB:

- Parquet median `0,1981s`, CSV `0,3288s`: Parquet nhanh hơn `1,66x` và nhỏ hơn
  khoảng `2,45x`; hash aggregate giống nhau.
- Cache giảm median mỗi action từ `0,2167s` xuống `0,1867s`, nhưng tính cả
  `1,1041s` materialize thì chưa hòa vốn trong ba lần dùng; ước tính khoảng 37
  lần reuse trong điều kiện đo hiện tại.
- Aggregate trên dữ liệu đã cache nhanh nhất với 1 partition, còn
  repartition/materialize nhanh nhất với 4 partitions. Kết quả này chỉ áp dụng
  cho truy vấn nhỏ trên `local[2]`.
- Status tracker ghi 4 Job, 8 stage ID, 7 task hoàn tất và 0 task thất bại; plan
  có `Exchange` do `groupBy`/`orderBy`.

Artifact:

- `outputs/benchmarks/phase8_benchmark.json`: số liệu và cấu hình máy đầy đủ.
- `outputs/benchmarks/representative_query_plan.txt`: logical, optimized và
  physical plan trước/sau Adaptive Query Execution.
- `docs/SPARK_EXECUTION_AND_BENCHMARK.md`: giải thích Partition → Task → Stage →
  Job, shuffle, cache và giới hạn benchmark.

## Dashboard trình bày — Phase 9

Tạo lại gói dữ liệu nhỏ cho dashboard từ các artifact đã có:

```powershell
.\.venv\Scripts\python.exe -m src.export_dashboard_data
```

Khởi động dashboard offline bằng một lệnh:

```powershell
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Mở `http://127.0.0.1:8501`. Bốn trang gồm Tổng quan, Mô hình, Cảnh báo và
Spark benchmark. Ứng dụng không khởi tạo Spark và không đọc Parquet/CSV lớn;
nó chỉ đọc JSON aggregate cùng tối đa 200 cảnh báo, 50 false positive và 50
false negative đã đóng gói. Xem hướng dẫn và giới hạn tại
`docs/DASHBOARD_GUIDE.md`.

Artifact:

- `outputs/dashboard/manifest.json`: data contract và đường dẫn nguồn nhỏ.
- `outputs/dashboard/alerts_sample.json`: mẫu cảnh báo/sai số có giới hạn.
- `dashboard/app.py`: ứng dụng Streamlit chạy không cần Internet.

## Structured Streaming mô phỏng — Phase 10

Chạy demo tự chứa: công bố hai file Parquet, xử lý thành hai micro-batch, dừng
Spark, công bố file thứ ba rồi restart bằng cùng checkpoint:

```powershell
.\.venv\Scripts\python.exe -m src.streaming_predict
```

Demo dùng schema tường minh, `maxFilesPerTrigger=1`, `AvailableNow` và nạp
PipelineModel cuối mà không train lại. Mỗi batch ghi prediction cùng summary số
cảnh báo; output theo `batch_id` giúp retry không nhân đôi batch đã ghi. Báo cáo
mới nhất nằm tại `outputs/metrics/phase10_streaming.json`.

Để xử lý thư mục file-source riêng và tiếp tục từ checkpoint ở lần gọi sau:

```powershell
.\.venv\Scripts\python.exe -m src.streaming_predict `
  --input-dir data\streaming_input\manual `
  --output-dir outputs\streaming\manual `
  --checkpoint-dir outputs\streaming\manual\checkpoint `
  --query-name unsw-nb15-manual
```

Các file Parquet đầu vào phải có schema UNSW-NB15 và được đặt nguyên tử vào thư
mục nguồn. Xem quy trình, data contract và giới hạn tại docs/STREAMING_GUIDE.md.

## Báo cáo và demo — Phase 11

Tài liệu nghiệm thu cuối đã tổng hợp kiến trúc, data lineage, chống leakage,
bảng model, official-test metrics, Spark benchmark, streaming và các giới hạn:
docs/BAO_CAO_KY_THUAT.md. Kịch bản trình bày 5–8 phút, checklist và phương án
dự phòng offline nằm tại docs/KICH_BAN_DEMO.md.

## Cấu trúc chính

```text
configs/             Cấu hình tập trung, seed và đường dẫn
data/raw/            CSV UNSW-NB15 gốc (không commit dữ liệu lớn)
data/bronze/         Parquet gần nguyên bản
data/silver/         Parquet đã làm sạch
data/samples/        Sample nhỏ cố định cho test
src/                 Mã nguồn PySpark
tests/               Kiểm thử theo phase
models/              PipelineModel đã lưu (không commit model nặng)
outputs/              Metrics, prediction, figure, benchmark và streaming
dashboard/            Ứng dụng Streamlit offline
docs/                 Nhật ký và tài liệu kỹ thuật
```

## Cấu hình

Mọi giá trị dùng chung nằm trong `configs/default.yaml`. Cấu hình mặc định dùng
`local[2]`, 4 shuffle partitions và 2 GB driver memory để phù hợp laptop.
