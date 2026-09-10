# Baseline Logistic Regression — Phase 5

Sinh lúc: `2026-09-10T20:38:00+07:00`.

## Phạm vi và chống leakage

Pipeline chỉ fit trên modeling train **140471** dòng và dự đoán validation **34870** dòng.
Official test **82332** dòng không được transform, evaluate hoặc dùng để chọn threshold.
Không dùng class weight hay tuning trong baseline; các thử nghiệm đó thuộc Phase 6.

## Kết quả validation tại threshold được chọn

Threshold **0.55** được chọn bằng F1 lớn nhất; nếu hòa thì ưu tiên Recall cao hơn, FPR thấp hơn và gần 0,5 hơn.

- Precision: **0.927048**.
- Recall: **0.982478**.
- F1-score: **0.953959**.
- False Positive Rate: **0.166200**.
- Accuracy (tham khảo): **0.935274**.
- ROC-AUC: **0.984068**.
- PR-AUC: **0.991605**.

### Confusion matrix

| Nhãn thật \ Dự đoán | Normal (0) | Attack (1) |
|---|---:|---:|
| Normal (0) | TN = 9231 | FP = 1840 |
| Attack (1) | FN = 417 | TP = 23382 |

## So sánh threshold mặc định 0,50

Tại 0,50: Precision `0.921933`, Recall `0.987478`, F1 `0.953581`, FPR `0.179749`.

## Đường threshold trên validation

| Threshold | Precision | Recall | F1 | FPR | TP | FP | TN | FN |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.10 | 0.9121 | 0.9979 | 0.9530 | 0.2068 | 23749 | 2290 | 8781 | 50 |
| 0.15 | 0.9133 | 0.9973 | 0.9534 | 0.2036 | 23734 | 2254 | 8817 | 65 |
| 0.20 | 0.9135 | 0.9966 | 0.9532 | 0.2029 | 23717 | 2246 | 8825 | 82 |
| 0.25 | 0.9145 | 0.9956 | 0.9533 | 0.2000 | 23694 | 2214 | 8857 | 105 |
| 0.30 | 0.9154 | 0.9948 | 0.9535 | 0.1975 | 23675 | 2187 | 8884 | 124 |
| 0.35 | 0.9165 | 0.9937 | 0.9535 | 0.1947 | 23648 | 2155 | 8916 | 151 |
| 0.40 | 0.9176 | 0.9924 | 0.9535 | 0.1917 | 23617 | 2122 | 8949 | 182 |
| 0.45 | 0.9193 | 0.9904 | 0.9535 | 0.1869 | 23571 | 2069 | 9002 | 228 |
| 0.50 | 0.9219 | 0.9875 | 0.9536 | 0.1797 | 23501 | 1990 | 9081 | 298 |
| 0.55 | 0.9270 | 0.9825 | 0.9540 | 0.1662 | 23382 | 1840 | 9231 | 417 |
| 0.60 | 0.9333 | 0.9750 | 0.9537 | 0.1499 | 23204 | 1659 | 9412 | 595 |
| 0.65 | 0.9428 | 0.9627 | 0.9526 | 0.1256 | 22911 | 1391 | 9680 | 888 |
| 0.70 | 0.9544 | 0.9436 | 0.9490 | 0.0970 | 22457 | 1074 | 9997 | 1342 |
| 0.75 | 0.9685 | 0.9166 | 0.9419 | 0.0640 | 21815 | 709 | 10362 | 1984 |
| 0.80 | 0.9799 | 0.8772 | 0.9257 | 0.0387 | 20876 | 429 | 10642 | 2923 |
| 0.85 | 0.9879 | 0.8266 | 0.9001 | 0.0217 | 19672 | 240 | 10831 | 4127 |
| 0.90 | 0.9944 | 0.7543 | 0.8579 | 0.0091 | 17951 | 101 | 10970 | 5848 |

## Vì sao Accuracy không đủ

Validation có tỷ lệ Attack `68.2506%`; chỉ đoán Attack đã có thể đạt accuracy tương đương tỷ lệ này. Accuracy cũng gộp FN và FP nên che khuất hai rủi ro khác nhau: bỏ sót tấn công và cảnh báo nhầm. Vì vậy baseline được đọc cùng Recall, FPR, PR-AUC, ROC-AUC và confusion matrix.

## Thời gian và khả năng tái lập

- Fit: **16.561 giây**.
- Predict validation: **1.670 giây**.
- Seed của split: `42`; Spark `4.2.0`, master `local[2]`.
- Logistic Regression chạy `50` iteration trên giới hạn `50`; đạt giới hạn: `True`.
