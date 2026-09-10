# Kịch bản demo 5–8 phút

Mục tiêu: chứng minh pipeline có dữ liệu thật, không leakage, model đã khóa,
Spark có execution evidence và output trình bày được. Không gọi đây là IDS
production hoặc live packet capture.

## Chuẩn bị trước giờ trình bày

Từ D:\Hoctap\big_data2:

~~~powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m src.export_dashboard_data
.\.venv\Scripts\streamlit.exe run dashboard\app.py
~~~

Mở dashboard ở http://127.0.0.1:8501. Chuẩn bị các file dự phòng đã có:
outputs/metrics/phase7_final_test.json,
outputs/benchmarks/phase8_benchmark.json,
outputs/metrics/phase10_streaming.json và docs/BAO_CAO_KY_THUAT.md.

## Kịch bản thời gian

| Thời gian | Thao tác | Câu nói chính | Bằng chứng |
|---:|---|---|---|
| 0:00–0:40 | Mở README/kiến trúc | Bài toán là phân loại network flow Normal/Attack với Spark MLlib, không phải bắt packet thật. | BAO_CAO_KY_THUAT §1–3 |
| 0:40–1:20 | Mở Data Dictionary/EDA dashboard | UNSW-NB15 có train 175.341 và test 82.332; label là target, attack_cat không là feature. | DATA_DICTIONARY, dashboard Tổng quan |
| 1:20–2:10 | Mở Model dashboard | Split hash/seed ngăn leakage; model/threshold chọn bằng validation trước khi test. | MODEL_COMPARISON |
| 2:10–3:00 | Nêu official-test result | Recall 0,987514 nhưng FPR 0,291595; không che bằng Accuracy và không tune lại theo test. | dashboard Mô hình, phase7_final_test.json |
| 3:00–3:50 | Mở Alert dashboard | Xem flow attack probability cao; đây là sample 200 alert, không phải toàn bộ output. | dashboard Cảnh báo |
| 3:50–4:50 | Mở Spark benchmark | Parquet median 0,1981s so với CSV 0,3288s; giải thích Exchange/shuffle, job/stage/task. | dashboard Spark benchmark |
| 4:50–5:50 | Chạy/cho xem streaming JSON | File-source micro-batch nạp model đã khóa; checkpoint restart chỉ xử lý file mới. | phase10_streaming.json |
| 5:50–6:30 | Chốt giới hạn | Local benchmark, nghiên cứu/offline, FPR test cao và streaming chưa production. | BAO_CAO_KY_THUAT §10 |

Nếu có 8 phút, dành thêm 1–2 phút mở
outputs/benchmarks/representative_query_plan.txt để chỉ trực tiếp Exchange, hoặc
chạy lệnh streaming demo bên dưới.

## Lệnh demo streaming tùy chọn

~~~powershell
.\.venv\Scripts\python.exe -m src.streaming_predict
~~~

Chờ JSON status: ok. Chỉ nêu các check có thật: ba batch 20 dòng, 60 ID phân
biệt, persistent query ID giống nhau sau restart, query run ID khác nhau và
only_new_file_processed_after_restart: true.

## Câu trả lời ngắn cho câu hỏi thường gặp

- **Vì sao không chỉ dùng Accuracy?** Train/test mất cân bằng và Accuracy gộp
  bỏ sót attack (FN) với cảnh báo nhầm (FP); vì vậy báo cáo Precision, Recall,
  F1, FPR, ROC-AUC, PR-AUC và confusion matrix.
- **Vì sao threshold 0,60?** Nó được chọn duy nhất trên validation với F1 cao
  nhất của candidate thắng; test được giữ đến Phase 7.
- **Spark đóng góp gì?** Schema + Parquet + lazy execution + partition/task +
  shuffle + ML pipeline thống nhất. Benchmark local chỉ là bằng chứng điều kiện,
  không tuyên bố Spark luôn nhanh hơn Pandas.
- **Có phải real-time không?** Không. Phase 10 chỉ phát lại file Parquet theo
  micro-batch, có checkpoint để minh họa cơ chế Structured Streaming.
- **Nếu FPR cao thì sao?** Không dùng model như bộ chặn tự động. Cần calibration,
  data mới, phân tích lỗi/cost và đánh giá lại trước production.

## Phương án dự phòng

| Sự cố | Cách xử lý trong demo | Không được làm |
|---|---|---|
| Dashboard không khởi động | Mở BAO_CAO_KY_THUAT và JSON artifact đã lưu. | Không bịa dữ liệu live. |
| Spark/JVM chậm | Dùng phase7_final_test, phase8_benchmark, phase10_streaming đã sinh. | Không chạy lại official test. |
| Không có Internet | Dashboard và artifact là local/offline. | Không tải dataset tại chỗ. |
| Hỏi về FPR | Chỉ rõ 29,16% test và giới hạn tổng quát hóa. | Không chọn threshold mới theo test. |

## Checklist nghiệm thu trước khi trình bày

- [ ] pytest -q exit code 0.
- [ ] Dashboard mở ở 127.0.0.1:8501.
- [ ] Metrics đọc đúng official-test JSON, không sửa tay.
- [ ] Nêu rõ threshold chọn trên validation, test chỉ đánh giá một lần.
- [ ] Nêu rõ sample alerts, benchmark local và streaming mô phỏng.
- [ ] Có sẵn JSON/report offline nếu giao diện hoặc Internet gặp sự cố.
