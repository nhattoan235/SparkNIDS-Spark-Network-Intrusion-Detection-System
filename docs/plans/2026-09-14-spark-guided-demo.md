# Spark Guided Demo Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Chuyển dashboard hiện tại thành demo có hướng dẫn 7 bước để người xem nhìn thấy ngay Apache Spark đang làm gì trong hệ thống phát hiện xâm nhập mạng, đồng thời giữ được nền tảng để phát triển tiếp thành đồ án cuối kỳ.

**Architecture:** Một tiến trình PySpark riêng chạy các thí nghiệm và ghi trạng thái/kết quả nhỏ theo hợp đồng JSON vào `outputs/demo/`. Streamlit chỉ đọc các JSON đó và trình bày một màn hình trung tâm theo luồng `Dữ liệu vào → Spark xử lý → Kết quả`, không khởi tạo Spark và không đọc dataset lớn. Batch demo dùng một SparkSession; bước Structured Streaming khởi động lại query với cùng checkpoint để chứng minh khả năng tiếp tục xử lý.

**Tech Stack:** Python 3.12, PySpark DataFrame/Spark SQL/MLlib/Structured Streaming, Streamlit, Altair, pytest, Streamlit AppTest, PowerShell.

---

## Phạm vi đã khóa

Demo gồm đúng 7 bước:

1. Dữ liệu mạng và schema.
2. CSV so với Parquet.
3. Lazy evaluation.
4. Job, Stage, Task, Partition và Shuffle.
5. Cache và tái sử dụng dữ liệu.
6. MLlib Random Forest phát hiện `Normal`/`Attack`.
7. Structured Streaming, micro-batch và checkpoint/restart.

Dashboard dùng thanh tiến trình ngang, không có sidebar. Mỗi bước chỉ hiển thị một flow trung tâm, tối đa ba metric, một khối “Bằng chứng Spark” và nút `Quay lại`, `Tiếp theo`, `Mở Spark UI`. Chế độ `demo` phải hoàn thành trong khoảng 8–10 phút trên máy hiện tại; chế độ `full` giữ số liệu chính thức của đồ án.

Không đánh giá lại official test, không ghi đè model cuối, không để dashboard chạy subprocess Spark, và không khẳng định Parquet/cache/nhiều partition luôn nhanh hơn trong mọi tình huống.

## Hợp đồng artifact mục tiêu

`outputs/demo/latest.json` là con trỏ nhỏ tới lần chạy mới nhất. `outputs/demo/runs/<run_id>/status.json` có cấu trúc ổn định:

```json
{
  "schema_version": 1,
  "run_id": "20260914-153000-demo",
  "mode": "demo",
  "status": "running",
  "active_step": "parquet",
  "spark_ui_url": "http://127.0.0.1:4040",
  "started_at": "ISO-8601",
  "updated_at": "ISO-8601",
  "steps": [
    {
      "id": "parquet",
      "index": 2,
      "title": "CSV và Parquet",
      "status": "succeeded",
      "flow": {"input": "UNSW-NB15", "spark": "read + filter + groupBy", "result": "cùng kết quả"},
      "metrics": [{"label": "CSV", "value": "0.52 s"}],
      "explanation": "...",
      "evidence": {"result_sha256_equal": true}
    }
  ],
  "errors": []
}
```

Mọi lần cập nhật phải ghi file tạm cùng thư mục rồi `Path.replace()` để dashboard không đọc trúng JSON đang viết dở. Dữ liệu lớn, model và DataFrame không được đưa vào JSON.

## Task 1: Khóa cấu hình demo và hợp đồng trạng thái

**Files:**

- Create: `src/demo_contract.py`
- Create: `tests/test_demo_contract.py`
- Modify: `configs/default.yaml`
- Modify: `.gitignore`
- Create: `outputs/demo/.gitkeep`

**Step 1: Viết test thất bại cho hợp đồng**

Test phải kiểm tra:

- Có đúng bảy step ID theo thứ tự: `data`, `parquet`, `lazy`, `execution`, `cache`, `mllib`, `streaming`.
- Trạng thái chỉ nhận `pending`, `running`, `succeeded`, `failed`, `skipped`.
- `write_status_atomic()` tạo JSON hợp lệ và lần ghi thứ hai thay hoàn toàn dữ liệu cũ.
- `write_latest_pointer()` trỏ tới `status.json` bằng đường dẫn tương đối từ project root.

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_demo_contract.py -q
```

Expected: FAIL vì `src.demo_contract` chưa tồn tại.

**Step 2: Cài đặt tối thiểu**

Trong `src/demo_contract.py`, thêm:

- `DEMO_STEP_IDS` và metadata tiếng Việt cho bảy bước.
- `new_demo_status(run_id, mode, spark_ui_url)`.
- `set_step_running(status, step_id)`, `set_step_result(...)`, `set_step_failed(...)`.
- `write_json_atomic(path, payload)` và `write_latest_pointer(output_root, status_path)`.
- Validation rõ lỗi khi step ID hoặc status không hợp lệ.

Trong `configs/default.yaml`, thêm:

```yaml
demo:
  output_root: "outputs/demo"
  mode: "demo"
  sample_train_rows: 12000
  sample_validation_rows: 3000
  random_forest_num_trees: 12
  random_forest_max_depth: 6
  measured_runs: 2
  partition_counts: [1, 2, 4]
  stream_rows_per_batch: 20
  hold_seconds: 120
```

Thêm `outputs/demo/*` và ngoại lệ `.gitkeep` vào `.gitignore`.

**Step 3: Chạy test**

Expected: PASS.

**Step 4: Commit checkpoint**

```powershell
git add src/demo_contract.py tests/test_demo_contract.py configs/default.yaml .gitignore outputs/demo/.gitkeep
git commit -m "feat: define guided Spark demo artifact contract"
```

## Task 2: Tách các thí nghiệm DataFrame dùng chung

**Files:**

- Create: `src/demo_experiments.py`
- Create: `tests/test_demo_experiments.py`
- Reuse: `src/benchmark.py`
- Reuse: `src/validate_data.py`

**Step 1: Viết test thất bại**

Dùng Spark fixture nhỏ để kiểm tra:

- `summarize_data(frame)` trả số dòng, số cột, schema và phân phối label.
- `run_format_comparison(...)` đọc CSV và Parquet, chạy cùng `representative_query()`, xác nhận cùng SHA-256 kết quả rồi mới so thời gian/kích thước.
- Hàm trả về dữ liệu JSON-serializable; không trả DataFrame hoặc Row.

Không assert “Parquet luôn nhanh hơn”; chỉ assert số đo dương và kết quả hai format giống nhau.

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_demo_experiments.py -q
```

Expected: FAIL vì module mới chưa tồn tại.

**Step 2: Cài đặt phần Data và Parquet**

Trong `src/demo_experiments.py`:

- Tái sử dụng `representative_query`, `result_signature`, `timing_summary` từ `src.benchmark`.
- `summarize_data()` chỉ collect các aggregate nhỏ.
- `run_format_comparison()` luân phiên thứ tự CSV/Parquet giữa các lần đo.
- Ghi rõ `schema_inferred_for_csv: false`, `schema_embedded_in_parquet: true`, codec/kích thước nếu đọc được.
- Trả `flow`, tối đa ba `metrics`, `explanation`, `evidence` đúng hợp đồng Task 1.

**Step 3: Chạy test mục tiêu và regression benchmark**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_demo_experiments.py tests\test_benchmark.py -q
```

Expected: PASS.

**Step 4: Commit checkpoint**

```powershell
git add src/demo_experiments.py tests/test_demo_experiments.py
git commit -m "feat: add data and format demo experiments"
```

## Task 3: Chứng minh lazy evaluation và mô hình thực thi Spark

**Files:**

- Modify: `src/demo_experiments.py`
- Modify: `tests/test_demo_experiments.py`

**Step 1: Viết test thất bại cho lazy evaluation**

Test tạo transformation `filter → groupBy → agg` dưới một `jobGroup` riêng và kiểm tra:

- Trước action: `job_ids_before_action == []`.
- Sau `collect()`: có ít nhất một Job.
- Kết quả có `logical_plan`, `physical_plan` và thời gian action.

**Step 2: Cài đặt `run_lazy_evaluation_demo()`**

- Dùng `spark.sparkContext.setJobGroup()` và `statusTracker().getJobIdsForGroup()`.
- Chụp plan trước action.
- Chỉ action `collect()` trên aggregate nhỏ.
- Xóa local property trong `finally`.
- Giải thích transformation chỉ dựng kế hoạch; action mới thực thi.

**Step 3: Viết test thất bại cho execution evidence**

Test `run_execution_demo()` phải trả:

- Số input partition.
- Danh sách Job, Stage và `num_tasks` từ status tracker.
- `shuffle_exchange_nodes >= 1` cho truy vấn có `groupBy/orderBy`.
- Tổng task hoàn tất và task lỗi.

**Step 4: Cài đặt `run_execution_demo()`**

Tách/đưa phần thu thập status tracker đang nằm trong `_tracked_collect()` của `src/benchmark.py` thành helper public có thể tái sử dụng, nhưng giữ tương thích để test Phase 8 vẫn chạy. Không parse giao diện HTML của Spark UI; dùng API status tracker và physical plan.

**Step 5: Chạy test**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_demo_experiments.py tests\test_benchmark.py -q
```

Expected: PASS và physical plan có `Exchange`.

**Step 6: Commit checkpoint**

```powershell
git add src/demo_experiments.py src/benchmark.py tests/test_demo_experiments.py tests/test_benchmark.py
git commit -m "feat: expose Spark lazy and execution evidence"
```

## Task 4: Làm thí nghiệm cache có diễn giải đúng

**Files:**

- Modify: `src/demo_experiments.py`
- Modify: `tests/test_demo_experiments.py`

**Step 1: Viết test thất bại**

`run_cache_demo()` phải tách ba số:

- Action không cache.
- Chi phí materialize cache lần đầu.
- Action tái sử dụng cache.

Test không phụ thuộc speedup lớn hơn 1; test chỉ kiểm tra cùng result signature, có gọi `unpersist()` và các duration không âm.

**Step 2: Cài đặt**

- Đọc cùng một Parquet và dùng cùng representative query.
- Đo warmup ngoài số lần chính.
- `persist()`, action materialize, đo reuse, rồi `unpersist(blocking=True)` trong `finally`.
- Tính `reuse_speedup` và `estimated_break_even_reuses` khi mẫu số hợp lệ; nếu không thì trả `null` cùng giải thích.
- UI text phải nói rõ cache chỉ đáng giá khi dữ liệu được tái sử dụng đủ nhiều.

**Step 3: Chạy test và commit**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_demo_experiments.py -q
git add src/demo_experiments.py tests/test_demo_experiments.py
git commit -m "feat: add honest Spark cache experiment"
```

## Task 5: Demo MLlib Random Forest không làm sai quy trình đồ án

**Files:**

- Modify: `src/demo_experiments.py`
- Create: `tests/test_demo_mllib.py`
- Reuse: `src/features.py`
- Reuse: `src/evaluate.py`

**Step 1: Viết test thất bại cho sampling và classifier**

Kiểm tra:

- `deterministic_demo_sample(frame, limit, seed)` luôn cho cùng tập ID và giữ cả hai label khi nguồn có đủ hai lớp.
- `build_demo_random_forest(config)` dùng `labelCol="label"`, đúng `featuresCol`, seed cấu hình, số cây/depth demo.
- Pipeline tạo bởi `build_binary_pipeline(config, classifier=...)` vẫn loại `id`, `attack_cat`, `label` khỏi feature vector.

**Step 2: Cài đặt `run_mllib_demo()`**

- Chỉ đọc `data/silver/modeling/train` và `validation`.
- Lấy mẫu xác định theo hash ID tới giới hạn cấu hình; không đọc `data/silver/test`.
- Tạo `RandomForestClassifier` 12 cây/depth 6 ở mode demo; mode full chỉ đọc artifact/model chính thức thay vì tự ý đánh giá lại test.
- Fit toàn bộ Spark ML Pipeline trên demo train, transform demo validation.
- Dùng `threshold_metrics(..., thresholds=[0.60])` và `ranking_metrics()` từ `src.evaluate`.
- Trả stage names, feature dimension, train/validation rows, fit seconds, confusion matrix, precision/recall/F1/FPR/ROC-AUC/PR-AUC.
- Đánh dấu rõ `purpose: educational_sample`, `official_test_used: false`, `model_saved: false`.

**Step 3: Test tích hợp trên sample nhỏ**

Dùng sample UNSW-NB15 đã commit hoặc DataFrame synthetic đủ schema; cấu hình RF rất nhỏ (`numTrees=2`, `maxDepth=2`) để test nhanh. Assert output có probability và hai lớp, không assert chất lượng mô hình.

**Step 4: Chạy test**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_demo_mllib.py tests\test_features.py tests\test_model_comparison.py -q
```

Expected: PASS; official-test guard không thay đổi.

**Step 5: Commit checkpoint**

```powershell
git add src/demo_experiments.py tests/test_demo_mllib.py
git commit -m "feat: add leakage-safe MLlib teaching demo"
```

## Task 6: Tích hợp Structured Streaming và checkpoint/restart

**Files:**

- Modify: `src/streaming_predict.py`
- Modify: `src/demo_experiments.py`
- Create: `tests/test_demo_streaming.py`
- Modify: `tests/test_streaming.py`

**Step 1: Viết test thất bại cho hai lượt query**

Test tạo ba file nguồn nhỏ:

- Lượt 1 công bố hai file và xử lý hai micro-batch.
- Dừng query, công bố file thứ ba.
- Lượt 2 dùng cùng checkpoint và chỉ xử lý file mới.
- Không tạo prediction trùng cho hai file cũ.

**Step 2: Mở helper streaming để tái sử dụng**

Đổi `_publish_demo_batch()` thành helper public có tên rõ nghĩa hoặc thêm wrapper public; giữ `run_phase10_demo()` chạy như cũ. Không thay data contract prediction hiện có.

**Step 3: Cài đặt `run_streaming_demo()`**

- Dùng thư mục theo `run_id` bên dưới `data/streaming_input/guided/` và `outputs/demo/runtime/<run_id>/streaming/`.
- Dùng `build_streaming_source()` và `run_available_now()` hiện có.
- Nạp model cuối đã khóa, không train lại trong streaming.
- Giữ `maxFilesPerTrigger=1` để mỗi file thể hiện thành một micro-batch.
- Trả `initial_batches`, `restart_batches`, tổng input/alert, checkpoint path, query progress rút gọn và bằng chứng file cũ không bị xử lý lại.

Nếu model cuối thiếu, chỉ step streaming chuyển `failed` với recovery command `python -m src.finalize_model`; status của các bước trước vẫn còn xem được.

**Step 4: Chạy test**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_demo_streaming.py tests\test_streaming.py -q
```

Expected: PASS.

**Step 5: Commit checkpoint**

```powershell
git add src/streaming_predict.py src/demo_experiments.py tests/test_demo_streaming.py tests/test_streaming.py
git commit -m "feat: add checkpoint restart to guided streaming demo"
```

## Task 7: Xây runner điều phối bảy bước và giữ Spark UI sống

**Files:**

- Create: `src/spark_demo.py`
- Create: `tests/test_spark_demo.py`
- Create: `scripts/run_demo.ps1`

**Step 1: Viết test thất bại cho CLI và failure isolation**

Kiểm tra parser hỗ trợ:

```text
--mode demo|full
--step all|data|parquet|lazy|execution|cache|mllib|streaming
--hold-seconds N
--config PATH
```

Mock experiment functions để assert:

- Các step chạy đúng thứ tự.
- Trước mỗi step, `active_step`/status được ghi ngay.
- Thành công được ghi `succeeded`.
- Exception ghi `failed`, kèm loại lỗi, thông báo ngắn và recovery hint; JSON vẫn hợp lệ.
- Chạy một step riêng không làm các step khác thành `failed`.

**Step 2: Cài đặt `src.spark_demo`**

- Preflight: config, Silver train/modeling split, CSV raw, Parquet, model cuối, output có quyền ghi, cổng Spark UI.
- Tạo một SparkSession cho bước 1–6, in URL Spark UI ra terminal và ghi URL vào status.
- Chạy từng experiment qua wrapper cập nhật trạng thái atomically.
- Bước 7 tái sử dụng SparkSession nếu API hiện có ổn định; nếu cần restart session để chứng minh khôi phục, dừng session batch có kiểm soát rồi tạo session streaming và giữ nguyên checkpoint.
- `--hold-seconds` giữ SparkSession sau bước batch để người thuyết trình mở Spark UI; in đếm ngược mỗi 10 giây, không busy-wait.
- Ctrl+C phải đi qua `finally`, giữ status tổng quát là `failed` (vì đây là một trạng thái kết thúc hợp lệ trong contract) và ghi `error.kind: interrupted`, nhưng vẫn `spark.stop()`.
- Không xóa artifact cũ; mỗi run có thư mục riêng và `latest.json` chỉ đổi sau khi status đầu tiên đã ghi thành công.

**Step 3: Thêm PowerShell launcher**

`scripts/run_demo.ps1` kiểm tra `.venv`, chạy:

```powershell
.\.venv\Scripts\python.exe -m src.spark_demo --mode demo --hold-seconds 120
```

Launcher chỉ điều phối lệnh; không sửa PATH toàn hệ thống.

**Step 4: Chạy test và CLI help**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_spark_demo.py -q
.\.venv\Scripts\python.exe -m src.spark_demo --help
```

Expected: PASS và help liệt kê đúng options.

**Step 5: Commit checkpoint**

```powershell
git add src/spark_demo.py tests/test_spark_demo.py scripts/run_demo.ps1
git commit -m "feat: orchestrate seven-step Spark teaching demo"
```

## Task 8: Tạo lớp đọc artifact và trạng thái fallback cho dashboard

**Files:**

- Modify: `dashboard/data_access.py`
- Create: `tests/fixtures/demo_status_complete.json`
- Create: `tests/fixtures/demo_status_failed.json`
- Modify: `tests/test_dashboard.py`

**Step 1: Viết test thất bại**

Kiểm tra `load_demo_status()`:

- Đọc run mà `latest.json` trỏ tới.
- Từ chối schema version không hỗ trợ và path thoát khỏi `outputs/demo`.
- Khi chưa có live run, tạo view model fallback từ các artifact Phase 3/6/8/10 hiện có.
- Khi JSON live bị thiếu giữa lúc thay file, trả lần đọc hợp lệ gần nhất hoặc thông báo `not_ready`, không làm toàn app crash.

**Step 2: Cài đặt**

- Thêm `DEMO_OUTPUT_ROOT`, `load_demo_status()`, `load_static_demo_fallback()` và validation.
- Cache chỉ file read ngắn; dùng TTL khoảng 1 giây cho live status để dashboard tự refresh thấy tiến trình.
- Không import PySpark trong `dashboard/`.

**Step 3: Chạy test**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_dashboard.py -q
```

Expected: PASS.

**Step 4: Commit checkpoint**

```powershell
git add dashboard/data_access.py tests/test_dashboard.py tests/fixtures/demo_status_complete.json tests/fixtures/demo_status_failed.json
git commit -m "feat: load live and fallback guided demo artifacts"
```

## Task 9: Thay dashboard nhiều tab bằng UI hướng dẫn tập trung

**Files:**

- Create: `dashboard/guided_demo.py`
- Create: `dashboard/styles.py`
- Modify: `dashboard/app.py`
- Modify: `.streamlit/config.toml`
- Modify: `tests/test_dashboard.py`

**Step 1: Viết AppTest thất bại cho navigation**

AppTest phải kiểm tra:

- Không có sidebar radio.
- Có bảy nhãn bước theo thứ tự.
- Mặc định ở bước 1.
- Nút `Tiếp theo` sang bước 2; `Quay lại` trở về bước 1.
- Ở bước 7 không vượt quá giới hạn.
- Mỗi bước có đúng một flow `Input → Spark → Result`, không quá ba metric.
- Artifact failed hiển thị lỗi và recovery hint nhưng app không exception.

**Step 2: Cài đặt các component thuần trình bày**

Trong `dashboard/guided_demo.py` tạo:

- `render_progress(current_step, steps)`.
- `render_flow(flow)`.
- `render_metrics(metrics)` giới hạn tối đa ba item.
- `render_spark_evidence(step)` cho plan, Job/Stage/Task, ML stages hoặc micro-batch tùy bước.
- `render_step(step)` và navigation bằng `st.session_state.demo_step`.

Trong `dashboard/styles.py`, gom CSS cho:

- `max-width` khoảng 1180px.
- Hero gọn một dòng.
- Stepper ngang rõ bước hiện tại/completed/pending.
- Card trung tâm có nhiều khoảng trắng, màu xanh Spark ở vùng xử lý và màu đỏ chỉ dành cho Attack/error.
- Responsive: dưới 900px flow xếp dọc, nút vẫn nhìn thấy.

**Step 3: Refactor `dashboard/app.py`**

- Bỏ radio/sidebar và bốn page cũ khỏi luồng chính.
- Header: tên demo, mode, trạng thái run, thời điểm cập nhật.
- `Mở Spark UI` dùng URL artifact; nếu Spark đã dừng thì ghi rõ UI chỉ có khi runner đang giữ session.
- Tự refresh nhẹ khi run `running`; không refresh khi complete/failed.
- Giữ một vùng “Xem kết quả đồ án” thu gọn ở bước MLlib thay vì đưa toàn bộ model metrics lên màn hình chính.
- Dashboard chỉ gọi lớp data access, không import Spark, không đọc CSV/Parquet.

**Step 4: Chạy AppTest**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_dashboard.py -q
```

Expected: PASS cho đủ bảy bước và fixture lỗi.

**Step 5: Kiểm tra giao diện ở ba kích thước**

Chạy:

```powershell
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Kiểm tra thủ công ở khoảng 1440×900, 1024×768 và 390×844:

- Tâm nhìn đầu tiên là flow, không phải bảng số.
- Không có scroll ngang.
- Ba metric không đẩy flow khỏi màn hình đầu.
- Nút navigation không đổi vị trí bất ngờ giữa các bước.

**Step 6: Commit checkpoint**

```powershell
git add dashboard/app.py dashboard/guided_demo.py dashboard/styles.py .streamlit/config.toml tests/test_dashboard.py
git commit -m "feat: replace dashboard tabs with guided Spark story"
```

## Task 10: Tài liệu hóa và đóng gói kịch bản demo

**Files:**

- Modify: `README.md`
- Rewrite: `docs/DASHBOARD_GUIDE.md`
- Rewrite: `docs/KICH_BAN_DEMO.md`
- Modify: `docs/SPARK_DEMO_REDESIGN.md`
- Modify: `tests/test_phase11_documents.py`

**Step 1: Viết test tài liệu thất bại**

Assert tài liệu có:

- Hai lệnh chạy runner/dashboard.
- URL 4040 và 8501.
- Bảy bước đúng thứ tự.
- Cảnh báo Spark UI biến mất sau khi SparkSession dừng.
- Phương án fallback khi live demo lỗi.
- Giới hạn local mode, file-source streaming và benchmark nhỏ.

**Step 2: Cập nhật hướng dẫn vận hành hai cửa sổ**

Terminal 1:

```powershell
.\scripts\run_demo.ps1
```

Terminal 2:

```powershell
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Trình duyệt:

- Dashboard: `http://127.0.0.1:8501`.
- Spark UI khi runner còn sống: `http://127.0.0.1:4040`.

Viết lời thoại 8–10 phút, mỗi bước gồm: người xem nhìn gì, Spark làm gì, liên hệ với IDS, câu chốt. Thêm checklist chạy thử trước buổi demo và cách chuyển sang artifact fallback nếu Spark không khởi động.

**Step 3: Chạy test tài liệu**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_phase11_documents.py -q
```

Expected: PASS.

**Step 4: Commit checkpoint**

```powershell
git add README.md docs/DASHBOARD_GUIDE.md docs/KICH_BAN_DEMO.md docs/SPARK_DEMO_REDESIGN.md tests/test_phase11_documents.py
git commit -m "docs: add guided Apache Spark demo runbook"
```

## Task 11: Nghiệm thu end-to-end

**Files:**

- Modify only if a defect is found in files above.
- Create: `outputs/demo/.gitkeep` remains the only committed file under demo output.

**Step 1: Chạy toàn bộ test**

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Expected: tất cả PASS; không có test nào đánh giá lại official test.

**Step 2: Chạy demo mode thật**

```powershell
.\scripts\run_demo.ps1
```

Expected:

- `outputs/demo/latest.json` xuất hiện.
- Bảy step lần lượt chuyển `pending → running → succeeded`.
- Spark UI mở được trong thời gian hold.
- MLlib báo `official_test_used: false`.
- Streaming lượt restart chỉ xử lý file mới.

**Step 3: Chạy dashboard cùng lúc**

```powershell
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Expected: dashboard tự nhận run mới nhất, navigation đủ bảy bước, không traceback, không đọc dữ liệu lớn.

**Step 4: Kiểm tra bằng chứng Spark**

Trong Spark UI xác nhận ít nhất:

- Tab Jobs có Job phát sinh sau action.
- Stage của truy vấn aggregate có Task theo partition.
- SQL/DataFrame plan có Exchange ở `groupBy/orderBy`.
- Storage chỉ có dữ liệu khi bước cache đã materialize.
- Structured Streaming progress có micro-batch tương ứng file input.

**Step 5: Rehearsal có bấm giờ**

Thực hiện toàn bộ lời thoại một lần. Mục tiêu 8–10 phút; nếu vượt, rút phần giải thích phụ, không bỏ bước lazy/execution/MLlib/streaming.

**Step 6: Kiểm tra thay đổi và commit cuối**

```powershell
git status --short
git diff --check
git commit -am "fix: polish guided Spark demo after rehearsal"
```

Chỉ tạo commit cuối nếu thật sự có sửa sau rehearsal. Không stage các artifact được sinh ra hoặc thay đổi tài liệu/samples cũ không thuộc kế hoạch.

## Tiêu chí hoàn thành

- Người chưa biết Spark nhìn màn hình đầu tiên hiểu được input nào đi vào, Spark đang làm gì và tạo kết quả gì.
- Bảy bước có bằng chứng Spark thật, không chỉ là biểu đồ dựng sẵn.
- Dashboard chạy được cả khi runner đang chạy và khi chỉ còn artifact offline.
- Demo MLlib không dùng official test và không thay model cuối.
- Streaming chứng minh checkpoint/restart không xử lý lại file cũ.
- Toàn bộ test pass và demo mode hoàn thành trong giới hạn thời gian dự kiến.
- Các giới hạn được nói rõ: local mode chưa phải cluster thật, file-source chưa phải Kafka/network live, benchmark nhỏ không đại diện mọi workload, model còn FPR cao và chưa sẵn sàng production.

## Thứ tự ưu tiên nếu thiếu thời gian

1. Runner + contract + bước lazy/execution.
2. UI bảy bước và fallback artifact.
3. CSV/Parquet + cache.
4. MLlib sample an toàn.
5. Streaming checkpoint/restart.
6. Polish responsive và rehearsal.

Không bỏ test contract, failure isolation hoặc guard không dùng official test để đổi lấy giao diện đẹp hơn.
