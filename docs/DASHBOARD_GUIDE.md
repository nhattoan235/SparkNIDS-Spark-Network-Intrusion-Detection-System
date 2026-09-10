# Hướng dẫn dashboard Phase 9

Dashboard dùng để trình bày kết quả nghiên cứu UNSW-NB15 đã sinh ở các phase
trước. Đây không phải hệ thống giám sát mạng production và không bắt packet
trực tiếp.

## Khởi động

Từ thư mục gốc `D:\Hoctap\big_data2`, nếu prediction hoặc metrics vừa thay đổi,
tạo lại gói dashboard:

```powershell
.\.venv\Scripts\python.exe -m src.export_dashboard_data
```

Sau đó chạy:

```powershell
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Mở `http://127.0.0.1:8501`. Dashboard chỉ dùng file cục bộ và vẫn demo được khi
không có Internet.

## Nội dung bốn trang

- **Tổng quan:** tổng flow, tỷ lệ Attack, protocol/service phổ biến, phân phối
  nhãn và nhóm tấn công từ Silver train.
- **Mô hình:** metrics official test, confusion matrix, so sánh candidate và các
  operating point ROC/PR theo threshold validation. ROC-AUC/PR-AUC chính xác là
  aggregate do Spark evaluator sinh; đường ROC/PR chỉ nối 17 threshold đã đo.
- **Cảnh báo:** tối đa 200 prediction Attack có xác suất cao nhất, có bộ lọc
  protocol, service và xác suất; thêm tối đa 50 false positive và 50 false
  negative để phân tích lỗi.
- **Spark benchmark:** kết quả CSV/Parquet, cache, partition và bằng chứng
  Job/Stage/Task của Phase 8.

## Data contract và giới hạn bộ nhớ

`outputs/dashboard/manifest.json` liệt kê các JSON nhỏ mà giao diện được phép
đọc. `src.export_dashboard_data` dùng Spark để aggregate hoặc `limit().collect()`
trước khi ghi JSON; nó không gọi `toPandas()` và kiểm tra số prediction nguồn
khớp metrics Phase 7. `dashboard/app.py` không import PySpark, không đọc dataset
Bronze/Silver và không đọc toàn bộ prediction Parquet.

Mẫu cảnh báo có chủ đích bị giới hạn để dashboard phản hồi nhanh. Muốn suy luận
dữ liệu mới, dùng `src.predict_batch` rồi chạy lại bước export; Phase 9 không có
form dự đoán một record vì đây là hạng mục tùy chọn và việc khởi tạo Spark cho
từng tương tác không phù hợp với chế độ demo offline nhẹ.

## Kiểm tra

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_dashboard.py -q
```

Test xác nhận bundle đầy đủ và có giới hạn, source không gọi `toPandas()`, đồng
thời render cả bốn trang bằng Streamlit AppTest mà không phát sinh exception.
