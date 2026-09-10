# So sánh mô hình và xử lý mất cân bằng — Phase 6

Sinh lúc: `2026-09-10T20:52:36+07:00`.

## Thiết kế thí nghiệm

Bốn candidate dùng cùng modeling train **140471** dòng và validation **34870** dòng. Official test **82332** dòng không được transform/evaluate.
Trọng số cân bằng chỉ được tính từ train theo `N/(2*n_class)`. Hai cấu hình Random Forest là tuning nhỏ có giới hạn, không phải grid search lớn.

## Bảng so sánh validation

| Candidate | Threshold | Precision | Recall | F1 | FPR | ROC-AUC | PR-AUC | Fit (s) | Predict (s) | Size (MB) | FP | FN |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| logistic_regression_unweighted | 0.55 | 0.9270 | 0.9825 | 0.9540 | 0.1662 | 0.9841 | 0.9916 | 18.727 | 1.735 | 0.017 | 1840 | 417 |
| logistic_regression_balanced | 0.40 | 0.9337 | 0.9756 | 0.9542 | 0.1489 | 0.9842 | 0.9919 | 12.731 | 1.018 | 0.017 | 1648 | 580 |
| random_forest_depth8 | 0.65 | 0.9280 | 0.9884 | 0.9573 | 0.1648 | 0.9840 | 0.9921 | 9.711 | 0.891 | 0.095 | 1824 | 276 |
| random_forest_depth12 **(chọn)** | 0.60 | 0.9472 | 0.9777 | 0.9622 | 0.1172 | 0.9902 | 0.9953 | 25.585 | 1.430 | 0.376 | 1297 | 530 |

## Lựa chọn mô hình

Chọn **random_forest_depth12** tại threshold **0.60** bằng F1 validation cao nhất; tie-break lần lượt là Recall cao hơn, FPR thấp hơn, predict/fit nhanh hơn.
Candidate được chọn có TP=23269, FP=1297, TN=9774, FN=530; Recall `0.977730` và FPR `0.117153`.

Quyết định chỉ dựa trên validation. Các thư mục model hiện tại là candidate để đo kích thước; Phase 7 sẽ huấn luyện/khóa pipeline cuối, đánh giá test đúng một lần và kiểm tra lưu-nạp.

## Trọng số lớp

- Normal (0): `1.563255`.
- Attack (1): `0.735127`.
