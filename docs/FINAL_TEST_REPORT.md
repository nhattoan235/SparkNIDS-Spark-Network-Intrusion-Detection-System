# Đánh giá official test và batch inference — Phase 7

Sinh lúc: `2026-09-10T21:09:26+07:00`; run ID `phase7-43839df4de0a`.

## Giao thức đánh giá

Model `unsw_nb15_random_forest_binary` và threshold **0.60** đã được khóa từ Phase 6 trên validation trước khi official test được mở.
Official test chỉ được evaluate tại threshold đã khóa; không khảo sát hoặc chọn lại threshold trên test.
PipelineModel được nạp từ đĩa trong tiến trình batch độc lập và không huấn luyện lại.

## Kết quả official test

- Số dòng: **82332**.
- Phân phối nhãn: Normal **37000**, Attack **45332** (`55,0599%` Attack).
- Precision: **0.805796**.
- Recall: **0.987514**.
- F1-score: **0.887448**.
- False Positive Rate: **0.291595**.
- Accuracy tham khảo: **0.862083**.
- ROC-AUC: **0.978606**.
- PR-AUC: **0.983752**.

### Confusion matrix

| Nhãn thật \ Dự đoán | Normal (0) | Attack (1) |
|---|---:|---:|
| Normal (0) | TN = 26211 | FP = 10789 |
| Attack (1) | FN = 566 | TP = 44766 |

## Validation so với test

Validation tại cùng threshold có F1 `0.962225`, Recall `0.977730`, FPR `0.117153`. Chênh lệch test được báo cáo như khả năng tổng quát hóa, không dùng để chỉnh lại model.

F1 test thấp hơn validation `0,074776` và FPR tăng `0,174442`. Đây là dấu hiệu
khác biệt phân phối/độ khó giữa hai split; kết quả được giữ nguyên, không chọn
lại threshold hay mô hình theo official test.

## Artifact batch

- Parquet: `D:\Hoctap\big_data2\outputs\predictions\final_test_parquet`.
- CSV: `D:\Hoctap\big_data2\outputs\predictions\final_test_csv`.
- Model SHA-256: `750231e41ffc674d31762f31082857ee49ad2f33a540ad42f0f0406074f93a2f`.
- Load model: `9.759s`; transform/count: `9.375s`; ghi output: `3.320s`.
