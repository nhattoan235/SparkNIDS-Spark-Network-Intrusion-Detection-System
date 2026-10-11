# UNSW-NB15 — nguồn dữ liệu và cách chuẩn bị

## Nguồn và kiểm tra tính toàn vẹn

Trang [UNSW-NB15 chính thức](https://research.unsw.edu.au/projects/unsw-nb15-dataset) mô tả bộ dữ liệu có 2.540.044 record. Trang UNSW hiện yêu cầu đăng nhập để tải file gốc; lượt triển khai này dùng [bản mirror trên Zenodo, record 10140548](https://zenodo.org/records/10140548), và kiểm tra đủ kích thước + MD5 công bố trước khi chạy Spark. SHA-256 thực tế cũng được ghi trong `data/full_unsw/source_manifest.json` trên ổ D.

| File | Dung lượng (byte) | MD5 đã xác minh | Dòng Spark đọc |
|---|---:|---|---:|
| `UNSW-NB15_1.csv` | 168.979.718 | `560dcc5981b3f6ab98027a16b053c9c1` | 700.001 |
| `UNSW-NB15_2.csv` | 165.221.021 | `59215357fbed7c76efc6a404a8607688` | 700.001 |
| `UNSW-NB15_3.csv` | 154.588.103 | `75e8567341edb760a23f803b920b69d4` | 700.001 |
| `UNSW-NB15_4.csv` | 97.588.754 | `f55875cab7f037fdad40b5b737e7db5e` | 440.044 |
| **Tổng** | **586.377.596** |  | **2.540.047** |

Các hash xác nhận nội dung tải về khớp file trên mirror. Tổng dòng đọc từ chính bốn file nhiều hơn con số tổng trên trang UNSW 3 dòng. Hệ thống không loại bỏ 3 dòng đó; nó ghi cả hai con số để người xem thấy rõ sai khác. Nếu hash hoặc số dòng của bản mirror khác, pipeline dừng và không tự dùng dữ liệu đó.

## Định dạng và quy tắc đọc

- CSV gốc không có hàng tiêu đề; mỗi record có 49 trường. Spark gắn schema tường minh theo thứ tự cột nguồn.
- Hai cột cổng `sport`/`dsport` đôi khi ghi số ở hệ thập lục phân như `0xc0a8`. Một số ô số để trống hoặc chỉ có khoảng trắng. Vì vậy Spark đọc token gốc dưới dạng text, sau đó chuyển kiểu bằng `try_cast`; token thiếu và token không hợp lệ được đếm trong audit thay vì làm job chết hoặc biến mất âm thầm.
- Bronze Parquet giữ 45 cột theo hợp đồng hiện có, thêm fingerprint, nguồn file và cờ chất lượng từng dòng. Dấu vết parse chỉ dùng kiểm toán, không làm feature của model.
- Chuẩn hóa giữ các feature trong hợp đồng 45 cột. IP, cổng, timestamp, ID, `attack_cat`, `label`, file nguồn và fingerprint không được đưa vào feature vector. `attack_cat` chỉ mô tả loại tấn công/nhãn để đánh giá; model học mục tiêu nhị phân `label`.
- `rate` được tính là `(Spkts + Dpkts) / dur`; khi thời lượng bằng 0 thì rate bằng 0.
- Các dòng có fingerprint giống nhau được giữ nguyên nhưng luôn vào cùng một split, để tránh cùng một flow xuất hiện ở train và test. Nếu cùng fingerprint có nhãn bình thường và tấn công mâu thuẫn, cả nhóm bị loại khỏi split và được đếm trong báo cáo.
- Các flow còn lại được chia xác định bằng seed thành khoảng 70% train, 15% validation, 15% test. Model fit trên train; threshold chọn bằng validation; test chỉ được chấm sau khi model và threshold đã khóa.

## Chạy trên máy cá nhân

Raw CSV, Parquet, model, log và kết quả đều nằm dưới:

`D:\Learning\bigdata\SparkNIDS-Spark-Network-Intrusion-Detection-System\data\full_unsw`

Mở PowerShell ở thư mục repo:

```powershell
. .\scripts\project_env.ps1
.\.venv\Scripts\python.exe -m src.full_unsw_pipeline --phase prepare
.\.venv\Scripts\python.exe -m src.full_unsw_pipeline --phase train
```

Hoặc chạy liên tục hai bước:

```powershell
. .\scripts\project_env.ps1
.\.venv\Scripts\python.exe -m src.full_unsw_pipeline --phase all
```

Nếu dữ liệu nằm ở ổ/thư mục khác, đặt `SPARKNIDS_FULL_UNSW_ROOT` hoặc dùng `--root`. Spark chạy `local[2]` trên một máy; đây là thử nghiệm quy mô lớn cho laptop, không phải cluster phân tán nhiều máy. Bản Docker Live Lab là mô phỏng streaming riêng và không dùng model UNSW này.

Mỗi lượt tạo thư mục riêng dưới `data/full_unsw/runs/<run-id>`; `latest.json` chỉ trỏ đến lượt mới. Lượt lỗi được giữ để tra log, không ghi đè kết quả cũ. Spark UI và số Job/Stage/Task chỉ có trong thời gian tiến trình Spark còn sống; trang kết quả giữ lại các số tổng hợp đã ghi.
