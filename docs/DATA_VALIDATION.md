# Báo cáo validation dữ liệu UNSW-NB15

Sinh lúc: `2026-09-10T19:38:29+07:00`

Nguồn chính thức: https://research.unsw.edu.au/projects/unsw-nb15-dataset

| Split | Rows | Columns | Duplicates | Invalid labels | SHA-256 | Bronze read-back |
|---|---:|---:|---:|---:|---|---:|
| train | 175341 | 45 | 0 | 0 | khớp | 175341 |
| test | 82332 | 45 | 0 | 0 | khớp | 82332 |

## Phân phối nhãn

- **train**: label=1: 119341, label=0: 56000
- **test**: label=1: 45332, label=0: 37000

## Ghi chú

- CSV được đọc bằng `StructType` tường minh, không dùng `inferSchema`.
- Bronze giữ nguyên 45 cột của designated train/test split và được ghi Snappy Parquet.
- Chi tiết schema, null từng cột và attack category nằm trong `outputs/metrics/phase2_validation.json`.
- Sample cố định có 200 dòng, cân bằng theo nhãn và chỉ dùng cho test.
