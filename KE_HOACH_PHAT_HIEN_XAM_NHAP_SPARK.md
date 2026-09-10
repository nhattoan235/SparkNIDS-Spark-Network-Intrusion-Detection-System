# Kế hoạch đồ án Apache Spark

## Tên đề tài

**Xây dựng hệ thống phát hiện xâm nhập mạng trên dữ liệu lớn bằng Apache Spark MLlib**

Tên tiếng Anh đề xuất:

**Building a Big Data Network Intrusion Detection System using Apache Spark MLlib**

---

## 1. Mục tiêu và câu hỏi nghiên cứu

### 1.1. Mục tiêu chính

Xây dựng một pipeline có thể:

1. Đọc và xử lý dữ liệu lưu lượng mạng lớn bằng Spark DataFrame/Spark SQL.
2. Phân tích sự khác nhau giữa lưu lượng bình thường và lưu lượng tấn công.
3. Tiền xử lý đồng nhất các thuộc tính số và phân loại bằng `pyspark.ml.Pipeline`.
4. Huấn luyện mô hình phân loại nhị phân `Normal`/`Attack`.
5. So sánh ít nhất hai mô hình Spark MLlib.
6. Đánh giá mô hình bằng các chỉ số phù hợp, đặc biệt chú ý Recall và False Positive Rate.
7. Lưu mô hình, nạp lại mô hình và dự đoán trên dữ liệu mới.
8. Trình bày kết quả qua dashboard và chứng minh các cơ chế xử lý phân tán của Spark.

### 1.2. Câu hỏi nghiên cứu

- Spark xử lý và phân vùng hàng triệu bản ghi lưu lượng mạng như thế nào?
- Đặc trưng nào giúp phân biệt lưu lượng bình thường và tấn công?
- Logistic Regression, Random Forest và/hoặc GBT khác nhau thế nào về chất lượng và thời gian huấn luyện?
- Có thể tăng Recall đối với tấn công mà vẫn kiểm soát số cảnh báo nhầm hay không?
- Số partition, cache và định dạng Parquet ảnh hưởng thế nào tới thời gian xử lý?

### 1.3. Sản phẩm cuối

- Source code PySpark có cấu trúc và chạy lại được.
- Dữ liệu đã làm sạch ở định dạng Parquet.
- Ít nhất hai mô hình và bảng so sánh kết quả.
- File dự đoán có xác suất, nhãn dự đoán và nhãn thật khi có.
- Dashboard trình bày thống kê, kết quả mô hình và các cảnh báo mẫu.
- Báo cáo kỹ thuật, hướng dẫn chạy và kịch bản thuyết trình.

---

## 2. Phạm vi để đề tài khả thi

### 2.1. Phần bắt buộc

- Ngôn ngữ: Python.
- Công nghệ chính: PySpark, Spark SQL và Spark MLlib (`pyspark.ml`).
- Chế độ xử lý chính: batch.
- Dữ liệu đề xuất: UNSW-NB15.
- Bài toán bắt buộc: phân loại nhị phân `Normal`/`Attack`.
- Mô hình bắt buộc:
  - Logistic Regression làm baseline.
  - Random Forest làm mô hình so sánh.
- Chỉ số bắt buộc:
  - Precision.
  - Recall.
  - F1-score.
  - ROC-AUC.
  - PR-AUC.
  - Confusion matrix.
  - False Positive Rate.
- Có lưu và nạp lại Spark ML PipelineModel.
- Có minh họa Partition, Transformation, Action, Job, Stage, Task, Executor, Cache và Shuffle.
- Có benchmark nhỏ để cho thấy tác động của partition/cache/Parquet.

### 2.2. Phần mở rộng

- Phân loại đa lớp theo `attack_cat` để nhận diện nhóm tấn công.
- GBTClassifier và điều chỉnh tham số.
- Structured Streaming dùng thư mục file làm nguồn để mô phỏng luồng mạng.
- Cơ chế sinh cảnh báo theo micro-batch.
- Phân tích feature importance của Random Forest/GBT.

### 2.3. Chưa làm ở phiên bản đầu

- Không bắt gói tin trực tiếp từ card mạng.
- Không triển khai Kafka trước khi pipeline batch hoàn chỉnh.
- Không triển khai Kubernetes, Hadoop cluster hoặc cloud nếu môn học không bắt buộc.
- Không dùng Deep Learning làm mô hình chính.
- Không dùng Pandas/scikit-learn thay cho Spark trong pipeline chính.
- Không khẳng định đây là hệ thống an ninh có thể dùng ngay trong sản xuất.

---

## 3. Dữ liệu đề xuất: UNSW-NB15

Nguồn chính thức:

- https://research.unsw.edu.au/projects/unsw-nb15-dataset

Đặc điểm phù hợp với đồ án:

- Có lưu lượng bình thường và lưu lượng tấn công.
- Có nhãn nhị phân và nhóm tấn công.
- Có nhiều thuộc tính số và phân loại, phù hợp để trình bày Spark ML Pipeline.
- Bản đầy đủ có khoảng 2,54 triệu bản ghi trong bốn file CSV.
- Có sẵn tập train/test nhỏ hơn để phát triển và kiểm thử nhanh.

### Chiến lược sử dụng dữ liệu

1. Dùng tập train/test được cung cấp để phát triển MVP nhanh.
2. Tách một phần tập train thành validation; không dùng test để chọn mô hình hoặc threshold.
3. Khi pipeline ổn định, chạy lại trên bốn CSV đầy đủ để chứng minh khả năng xử lý dữ liệu lớn.
4. Chuyển dữ liệu CSV sang Parquet và so sánh thời gian đọc/xử lý.
5. Lưu bản mẫu nhỏ trong repository; không commit toàn bộ dữ liệu lớn hoặc model nặng.

### Quy tắc chống rò rỉ dữ liệu

- Chỉ `fit` StringIndexer, OneHotEncoder, scaler và mô hình trên tập train.
- Không dùng `label` hoặc `attack_cat` làm feature.
- Không cân bằng dữ liệu trước khi chia train/validation/test.
- Không lựa chọn threshold trên test set.
- Rà soát các cột định danh hoặc cột có thể tiết lộ trực tiếp nhãn.
- Nếu tạo dữ liệu mô phỏng cho streaming, phải ghi rõ đó là dữ liệu phát lại/mô phỏng.

---

## 4. Kiến trúc tổng thể

```text
UNSW-NB15 CSV
      |
      v
Spark Ingestion + Schema Validation
      |
      v
Bronze Parquet (dữ liệu gần nguyên bản)
      |
      v
Cleaning + Type Casting + Invalid-value Handling
      |
      v
Silver Parquet (dữ liệu sạch)
      |
      +----------------------+
      |                      |
      v                      v
Spark SQL / EDA       Spark ML Pipeline
                             |
          StringIndexer -> OneHotEncoder
          Numeric handling -> VectorAssembler
                             |
              Logistic Regression / Random Forest
                             |
                             v
                 Validation + Threshold selection
                             |
                             v
                    Final Test Evaluation
                             |
                             v
                 Saved Model + Batch Predictions
                             |
                             v
                    Streamlit Dashboard
```

Phần mở rộng:

```text
Thư mục chứa các file micro-batch
              |
              v
Spark Structured Streaming
              |
              v
Saved PipelineModel.transform()
              |
              v
Cảnh báo + thống kê theo cửa sổ thời gian
```

---

## 5. Cấu trúc thư mục đề xuất

```text
big_data2/
|-- README.md
|-- requirements.txt
|-- .gitignore
|-- configs/
|   `-- default.yaml
|-- data/
|   |-- raw/
|   |-- bronze/
|   |-- silver/
|   |-- streaming_input/
|   `-- samples/
|-- src/
|   |-- __init__.py
|   |-- spark_session.py
|   |-- ingest.py
|   |-- validate_data.py
|   |-- preprocess.py
|   |-- eda.py
|   |-- features.py
|   |-- train_binary.py
|   |-- train_multiclass.py
|   |-- evaluate.py
|   |-- predict_batch.py
|   |-- benchmark.py
|   `-- streaming_predict.py
|-- dashboard/
|   `-- app.py
|-- tests/
|   |-- test_validation.py
|   |-- test_preprocess.py
|   |-- test_features.py
|   `-- test_model_smoke.py
|-- models/
|-- outputs/
|   |-- metrics/
|   |-- predictions/
|   |-- figures/
|   |-- benchmarks/
|   `-- dashboard/
`-- docs/
    |-- BAO_CAO_KY_THUAT.md
    |-- KICH_BAN_DEMO.md
    |-- NHAT_KY_THUC_HIEN.md
    `-- DATA_DICTIONARY.md
```

---

## 6. Các giai đoạn thực hiện

## Giai đoạn 0 — Chốt yêu cầu và tiêu chí đánh giá

**Thời gian:** 30–60 phút

### Công việc

- Chốt tên đề tài, phạm vi batch bắt buộc và phần mở rộng.
- Kiểm tra yêu cầu của giảng viên về báo cáo, số lượng thành viên và cluster.
- Chốt tiêu chí thành công của mô hình và tiêu chí thành công của hệ thống.

### Hoàn thành khi

- Có tên đề tài chính thức.
- Phân biệt rõ đề tài này với phát hiện bất thường trong log hệ thống.
- Có danh sách đầu ra bắt buộc.

## Giai đoạn 1 — Khởi tạo môi trường và bộ khung

**Thời gian:** 1–2 giờ

### Công việc

- Kiểm tra Java, Python và PySpark trên Windows.
- Tạo cấu trúc dự án trực tiếp trong `D:\Hoctap\big_data2` theo cấu trúc trên.
- Tạo SparkSession với cấu hình local an toàn.
- Tạo `requirements.txt`, cấu hình YAML và README ban đầu.
- Viết smoke test tạo DataFrame và chạy một action.

### Hoàn thành khi

- `spark-submit` hoặc lệnh Python tương đương chạy thành công.
- Spark UI xuất hiện trong lúc job chạy.
- Smoke test thành công.

## Giai đoạn 2 — Thu nhận, kiểm tra và chuẩn hóa dữ liệu

**Thời gian:** 2–4 giờ

### Công việc

- Tải dữ liệu từ nguồn chính thức hoặc ghi rõ cách tải thủ công.
- Xây schema tường minh thay vì phụ thuộc hoàn toàn vào `inferSchema`.
- Kiểm tra số dòng, số cột, kiểu dữ liệu, null, trùng lặp và nhãn.
- Chuẩn hóa tên cột.
- Chuyển dữ liệu thô sang Bronze Parquet.
- Tạo sample nhỏ cố định phục vụ test.

### Hoàn thành khi

- Có báo cáo validation dạng JSON/Markdown.
- Số dòng và phân phối nhãn được ghi lại.
- Bronze Parquet đọc lại được bằng Spark.

## Giai đoạn 3 — Làm sạch và EDA bằng Spark

**Thời gian:** 2–3 giờ

### Công việc

- Xử lý null, giá trị vô hạn, kiểu dữ liệu sai và bản ghi không hợp lệ.
- Thống kê `Normal`/`Attack` và từng `attack_cat`.
- Phân tích protocol, service, state và các đặc trưng số quan trọng.
- Dùng Spark SQL cho ít nhất ba truy vấn phân tích.
- Tạo Silver Parquet.
- Chỉ `collect()` các bảng tổng hợp nhỏ phục vụ biểu đồ.

### Hoàn thành khi

- Có bảng phân phối lớp và một số biểu đồ EDA.
- Có tài liệu giải thích mất cân bằng dữ liệu.
- Dữ liệu Silver ổn định và tái sử dụng được.

## Giai đoạn 4 — Xây dựng Spark ML Pipeline

**Thời gian:** 3–5 giờ

### Công việc

- Xác định cột số, cột phân loại và cột bị loại.
- Dùng `StringIndexer(handleInvalid="keep")` cho thuộc tính phân loại.
- Dùng `OneHotEncoder` khi phù hợp.
- Dùng `VectorAssembler` tạo vector đặc trưng.
- Scale feature cho Logistic Regression nếu cần.
- Đóng gói toàn bộ preprocessing và model trong một `Pipeline`.
- Tách train/validation/test đúng quy tắc.

### Hoàn thành khi

- Một pipeline duy nhất có thể `fit(train)` và `transform(validation)`.
- Dữ liệu validation/test không được dùng để fit transformer.
- Có test cho schema đầu ra và vector đặc trưng.

## Giai đoạn 5 — Baseline Logistic Regression

**Thời gian:** 2–3 giờ

### Công việc

- Huấn luyện Logistic Regression nhị phân.
- Tạo confusion matrix và toàn bộ chỉ số bắt buộc.
- Đánh giá theo nhiều threshold trên validation.
- Ghi thời gian fit, thời gian predict và cấu hình Spark.

### Hoàn thành khi

- Có baseline tái lập được với seed cố định.
- Có lý giải vì sao Accuracy không đủ.
- Có threshold mặc định được chọn từ validation, không phải test.

## Giai đoạn 6 — Mô hình so sánh và xử lý mất cân bằng

**Thời gian:** 3–5 giờ

### Công việc

- Huấn luyện Random Forest.
- Thử `weightCol` hoặc một chiến lược cân bằng hợp lý nếu cần.
- Tuning nhỏ, có giới hạn; tránh grid search quá lớn trên laptop.
- GBTClassifier là tùy chọn sau khi Random Forest ổn định.
- So sánh chất lượng, thời gian và kích thước model.

### Hoàn thành khi

- Có bảng so sánh ít nhất hai mô hình.
- Có lựa chọn mô hình cuối kèm lý do.
- Có kiểm tra cảnh báo nhầm và tấn công bị bỏ sót.

## Giai đoạn 7 — Đánh giá cuối, lưu model và batch inference

**Thời gian:** 2–4 giờ

### Công việc

- Khóa pipeline, tham số và threshold sau validation.
- Đánh giá test set đúng một lần cho kết quả cuối.
- Lưu `PipelineModel`.
- Nạp lại model trong tiến trình khác và dự đoán dữ liệu mới.
- Xuất Parquet/CSV gồm prediction, probability và các trường mô tả cần thiết.
- Nếu làm đa lớp, báo cáo macro-F1 và per-class recall.

### Hoàn thành khi

- Model lưu/nạp thành công.
- Batch inference chạy không cần huấn luyện lại.
- Metrics và predictions có metadata về phiên chạy.

## Giai đoạn 8 — Chứng minh giá trị của Apache Spark

**Thời gian:** 2–4 giờ

### Công việc

- Dùng `explain()` để lưu logical/physical plan của truy vấn tiêu biểu.
- Chụp Spark UI hoặc ghi lại Job/Stage/Task của một lần chạy.
- Giải thích shuffle trong groupBy/join và khi huấn luyện.
- Benchmark có kiểm soát:
  - CSV so với Parquet.
  - cache và không cache khi tái sử dụng DataFrame.
  - một vài mức partition hợp lý.
- Ghi cấu hình máy để tránh diễn giải sai benchmark.

### Hoàn thành khi

- Trả lời được tại sao đề tài cần Spark thay vì chỉ dùng Pandas.
- Có bảng benchmark tái lập được.
- Có sơ đồ liên hệ Partition → Task → Core → Stage → Job.

## Giai đoạn 9 — Dashboard trình bày

**Thời gian:** 3–5 giờ

### Công việc

- Tạo Streamlit dashboard đọc kết quả tổng hợp nhỏ.
- Trang tổng quan: tổng flow, tỷ lệ attack, protocol/service phổ biến.
- Trang mô hình: confusion matrix, ROC/PR, bảng so sánh model.
- Trang cảnh báo: danh sách flow được dự đoán nguy hiểm và xác suất.
- Tùy chọn form nhập một record mẫu để chạy prediction.

### Hoàn thành khi

- Dashboard khởi động bằng một lệnh rõ ràng.
- Không `toPandas()` toàn bộ dữ liệu lớn.
- Demo được bằng dữ liệu đã đóng gói, kể cả khi không có Internet.

## Giai đoạn 10 — Structured Streaming tùy chọn

**Thời gian thêm:** 3–5 giờ

### Công việc

- Phát lại các record thành nhiều file micro-batch.
- Dùng `readStream` với schema cố định.
- Áp dụng `PipelineModel` đã huấn luyện; không train lại trong stream.
- Ghi output và checkpoint.
- Tổng hợp số cảnh báo theo batch hoặc cửa sổ thời gian.

### Hoàn thành khi

- Record mới xuất hiện trong source sẽ tạo prediction mới.
- Restart không làm mất checkpoint.
- Báo cáo gọi đây là mô phỏng streaming, không phải live packet capture.

## Giai đoạn 11 — Báo cáo và chuẩn bị thuyết trình

**Thời gian:** 4–6 giờ

### Công việc

- Hoàn thiện README và báo cáo kỹ thuật.
- Vẽ kiến trúc hệ thống và Spark execution flow.
- Ghi hạn chế của dữ liệu và mô hình.
- Chuẩn bị kịch bản demo 5–8 phút.
- Chạy lại quy trình từ môi trường sạch hoặc theo hướng dẫn README.

### Hoàn thành khi

- Người khác có thể chạy theo README.
- Số liệu trong báo cáo khớp file metrics.
- Có phương án demo dự phòng bằng output đã lưu.

---

## 7. Ước lượng tiến độ

### MVP có thể chạy

Khoảng **12–18 giờ**, gồm:

- Môi trường và cấu trúc.
- Dữ liệu và EDA cơ bản.
- Pipeline Logistic Regression + Random Forest.
- Đánh giá, lưu model và batch prediction.
- README cơ bản.

### Bản hoàn chỉnh để trình bày

Khoảng **22–32 giờ**, gồm thêm:

- Benchmark Spark.
- Dashboard.
- Báo cáo, sơ đồ và kịch bản demo.
- Kiểm thử và làm sạch sản phẩm.

### Phần mở rộng

- Multiclass: thêm 3–5 giờ.
- Structured Streaming: thêm 3–5 giờ.
- Kafka hoặc triển khai cluster thật: chỉ thực hiện khi có yêu cầu riêng.

### Lịch 12 phiên gợi ý

Mỗi phiên 60–90 phút:

1. Kiểm tra môi trường và tạo cấu trúc.
2. Thu nhận dữ liệu và schema.
3. Validation và chuyển Parquet.
4. EDA bằng Spark SQL.
5. Feature pipeline.
6. Logistic Regression baseline.
7. Random Forest.
8. Metrics, threshold và error analysis.
9. Lưu model và batch prediction.
10. Benchmark Spark.
11. Dashboard.
12. Báo cáo và diễn tập demo.

---

## 8. Tiêu chí nghiệm thu cuối

- [ ] Spark là engine chính trong ingest, ETL, feature engineering, training và prediction.
- [ ] Có schema và data validation.
- [ ] Có Bronze/Silver Parquet.
- [ ] Có train/validation/test không rò rỉ.
- [ ] Có Logistic Regression và Random Forest.
- [ ] Có Precision, Recall, F1, ROC-AUC, PR-AUC, FPR và confusion matrix.
- [ ] Không sử dụng Accuracy làm kết luận duy nhất.
- [ ] Có lựa chọn threshold dựa trên validation.
- [ ] Có lưu/nạp `PipelineModel`.
- [ ] Có batch inference end-to-end.
- [ ] Có `explain()`, Spark UI hoặc bằng chứng Job/Stage/Task.
- [ ] Có benchmark CSV/Parquet và cache/partition ở phạm vi hợp lý.
- [ ] Có dashboard không kéo toàn bộ dữ liệu về driver.
- [ ] Có README, báo cáo và kịch bản demo.
- [ ] Mọi số liệu trong báo cáo truy ngược được tới output của code.

---

## 9. Rủi ro và cách kiểm soát

| Rủi ro | Cách kiểm soát |
|---|---|
| Cài Spark/Java trên Windows lỗi | Kiểm tra phiên bản trước; tạo smoke test; ghi rõ biến môi trường |
| Dữ liệu đầy đủ quá nặng | Phát triển trên official train/test; chạy full data sau khi pipeline ổn định |
| Accuracy cao nhưng bỏ sót tấn công | Ưu tiên Recall, PR-AUC, F1, FPR và confusion matrix |
| Data leakage | Fit transformer trên train; chọn threshold trên validation; test chỉ dùng cuối |
| Grid search quá lâu | Dùng TrainValidationSplit hoặc danh sách tham số nhỏ |
| Dashboard hết RAM | Chỉ đọc aggregate/prediction đã giới hạn; không collect toàn bộ dataset |
| Đề tài bị hiểu giống log anomaly | Nhấn mạnh input là network flow có nhãn và bài toán supervised classification |
| Streaming làm trễ tiến độ | Hoàn thiện batch trước; streaming là mục cộng điểm |

---

## 10. Kịch bản thuyết trình ngắn

1. **Bài toán:** dữ liệu mạng lớn, khó kiểm tra thủ công.
2. **Dữ liệu:** mô tả UNSW-NB15, nhãn và các nhóm feature.
3. **Kiến trúc:** CSV → Bronze/Silver Parquet → ML Pipeline → Prediction → Dashboard.
4. **Spark hoạt động:** partition tạo task; stage bị chia tại shuffle; executor chạy task.
5. **Mô hình:** baseline Logistic Regression và Random Forest.
6. **Đánh giá:** nhấn mạnh Recall, FPR, PR-AUC và confusion matrix.
7. **Demo:** chạy batch prediction và xem cảnh báo trên dashboard.
8. **Big Data:** trình bày Spark UI/physical plan và benchmark.
9. **Hạn chế:** dữ liệu nghiên cứu, chưa phải IDS production, chưa bắt packet thật.

---

## 11. Prompt bàn giao cho hội thoại Codex mới

Sao chép nguyên khối dưới đây vào hội thoại mới:

```text
Bạn hãy tiếp quản và triển khai đầy đủ đồ án:

“Xây dựng hệ thống phát hiện xâm nhập mạng trên dữ liệu lớn bằng Apache Spark MLlib”.

Workspace bắt buộc:
D:\Hoctap\big_data2

Tài liệu kế hoạch nguồn:
D:\Hoctap\big_data2\KE_HOACH_PHAT_HIEN_XAM_NHAP_SPARK.md

Trước khi sửa hoặc tạo code:
1. Đọc toàn bộ tài liệu kế hoạch trên.
2. Kiểm kê workspace, README, file trạng thái, source code và các thay đổi hiện có.
3. Kiểm tra Java, Python, PySpark, SparkSession và tài nguyên máy; không giả định môi trường đã đúng.
4. Xác định phase đầu tiên chưa hoàn thành rồi triển khai từ đó; không làm lại phần đã hoàn tất nếu không cần thiết.

Mục tiêu bắt buộc:
- Dùng PySpark DataFrame, Spark SQL và pyspark.ml làm công nghệ chính.
- Dùng UNSW-NB15 từ nguồn chính thức; không tự nhận dữ liệu mô phỏng là dữ liệu thật.
- Xây pipeline batch end-to-end:
  ingest -> validate -> Bronze Parquet -> clean -> Silver Parquet -> EDA -> feature pipeline -> train -> validate -> evaluate -> save model -> load model -> batch predict.
- Bài toán chính là phân loại nhị phân Normal/Attack.
- Logistic Regression là baseline và Random Forest là mô hình so sánh.
- GBT và phân loại đa lớp attack_cat chỉ là mở rộng sau khi bản bắt buộc ổn định.
- Đánh giá bằng Precision, Recall, F1, ROC-AUC, PR-AUC, False Positive Rate và confusion matrix; không kết luận chỉ bằng Accuracy.
- Chọn model, hyperparameter và decision threshold trên validation set. Chỉ dùng test set cho đánh giá cuối.
- Ngăn data leakage: mọi StringIndexer, OneHotEncoder, scaler, VectorAssembler liên quan và model phải nằm trong Spark Pipeline fit trên train; không dùng label/attack_cat làm feature; không resample trước khi split.
- Lưu và nạp lại PipelineModel; batch prediction phải chạy mà không train lại.
- Dùng explain(), Spark UI hoặc output phù hợp để chứng minh Job, Stage, Task, Partition, Executor và Shuffle.
- Thực hiện benchmark nhỏ, có kiểm soát cho CSV/Parquet, cache/no-cache và số partition hợp lý.
- Dashboard Streamlit chỉ đọc bảng tổng hợp hoặc prediction đã giới hạn; tuyệt đối không toPandas/collect toàn bộ dữ liệu lớn.
- Batch hoàn chỉnh trước. Structured Streaming file-source là phần mở rộng. Không triển khai Kafka, Kubernetes, cloud hoặc live packet capture trước khi phần cốt lõi đạt tiêu chí nghiệm thu.

Yêu cầu tổ chức code:
- Tạo hoặc tiếp tục dự án trực tiếp tại D:\Hoctap\big_data2.
- Cấu hình, đường dẫn và seed phải tập trung, không rải magic values trong code.
- Code, tên biến và tên file dùng tiếng Anh; tài liệu giải thích có thể dùng tiếng Việt.
- Mỗi phase phải có kiểm thử hoặc smoke test tương xứng.
- Không commit dataset/model/output lớn. Cung cấp script hoặc hướng dẫn tải dữ liệu và sample nhỏ cho test.
- Bảo toàn file người dùng và thay đổi không liên quan; không xóa hoặc ghi đè tùy tiện.

Yêu cầu theo dõi tiến độ:
- Sau mỗi phase, cập nhật:
  D:\Hoctap\big_data2\docs\NHAT_KY_THUC_HIEN.md
- Nhật ký phải ghi: việc đã làm, file thay đổi, lệnh kiểm thử, kết quả, vấn đề còn lại và phase tiếp theo.
- README phải luôn phản ánh các lệnh thực sự chạy được.
- Không tuyên bố hoàn thành trước khi pipeline end-to-end chạy, model load lại được, metrics được sinh từ code và hướng dẫn chạy đã được kiểm chứng.

Quyền tự chủ:
- Chủ động thực hiện các bước trong phạm vi kế hoạch và tự chọn phương án kỹ thuật an toàn, đơn giản nhất.
- Không dừng để hỏi các chi tiết nhỏ có thể xác minh từ workspace hoặc tài liệu chính thức.
- Chỉ dừng hỏi khi thiếu dataset không thể tải/thay thế hợp lệ, cần quyền truy cập, có xung đột với dữ liệu người dùng, hoặc cần thay đổi lớn phạm vi đề tài.
- Nếu môi trường chặn một bước, ghi bằng chứng lỗi, thử phương án an toàn trong phạm vi rồi tiếp tục phần độc lập khác.

Thứ tự ưu tiên khi thiếu thời gian:
1. Batch pipeline end-to-end.
2. Chống leakage và đánh giá đúng.
3. Lưu/load model và batch inference.
4. Chứng minh Spark bằng physical plan, Spark UI và benchmark.
5. Dashboard.
6. Báo cáo và kịch bản demo.
7. Multiclass và Structured Streaming.
8. Kafka/cluster thật chỉ khi được yêu cầu.

Bắt đầu ngay bằng cách đọc toàn bộ kế hoạch, kiểm kê workspace, kiểm tra môi trường và triển khai phase đầu tiên chưa hoàn thành. Hãy làm việc cho tới khi hoàn thành phần có thể làm an toàn trong lượt hiện tại, sau đó báo cáo ngắn gọn kết quả kiểm chứng và bước kế tiếp.
```

---

## 12. Prompt ngắn nếu chỉ muốn chạy một phase

```text
Hãy tiếp tục đồ án tại D:\Hoctap\big_data2.
Đọc D:\Hoctap\big_data2\KE_HOACH_PHAT_HIEN_XAM_NHAP_SPARK.md và
D:\Hoctap\big_data2\docs\NHAT_KY_THUC_HIEN.md, xác định phase đầu tiên
chưa hoàn thành, triển khai phase đó, chạy kiểm thử phù hợp và cập nhật nhật ký.
Không làm lại phần đã hoàn tất và không mở rộng sang streaming/Kafka khi batch
pipeline chưa đạt tiêu chí nghiệm thu.
```
