# Nguồn dữ liệu UNSW-NB15

Nguồn chuẩn của đồ án là trang [UNSW-NB15 của UNSW](https://research.unsw.edu.au/projects/unsw-nb15-dataset).
Trang này xác nhận designated training set có 175.341 bản ghi và testing set có
82.332 bản ghi. Quyền dùng miễn phí chỉ áp dụng cho nghiên cứu học thuật; báo cáo
cuối phải trích dẫn các công trình do UNSW liệt kê.

## Cách lấy dữ liệu

Liên kết `HERE` trên trang chính thức hiện chuyển tới SharePoint và có thể yêu
cầu tài khoản Microsoft/UNSW. Nếu truy cập được, tải thủ công hai file sau vào
`data/raw/`:

- `UNSW_NB15_training-set.csv`
- `UNSW_NB15_testing-set.csv`

Để phase có thể chạy tái lập khi SharePoint chặn truy cập ẩn danh, cấu hình hiện
dùng mirror `Mouwiya/UNSW-NB15-small` trên Hugging Face. Đây được ghi rõ là
**mirror, không phải nguồn chính thức**. File chỉ được chấp nhận khi checksum
khớp:

| File | SHA-256 |
|---|---|
| `UNSW_NB15_training-set.csv` | `bec7dd5ec88dc2a0ccc7a07879d338395ed7421750f675fd0339e07dfe0648fa` |
| `UNSW_NB15_testing-set.csv` | `734fe6642edf758f7c94d7d9149426b49d202fe8e7bf0bef47392489c3c0a559` |

Lệnh tải và kiểm tra:

```powershell
.\.venv\Scripts\python.exe -m src.download_data
```

Không commit hai CSV, Bronze Parquet hoặc dataset đầy đủ vào repository.

