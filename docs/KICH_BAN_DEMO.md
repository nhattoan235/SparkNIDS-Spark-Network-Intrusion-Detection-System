# Kịch bản guided demo Apache Spark · 8–10 phút

Mục tiêu của phần demo là trả lời một câu hỏi duy nhất:

> Trong hệ thống phát hiện xâm nhập mạng, Apache Spark thực sự làm gì?

Mỗi bước trên dashboard đều nói theo mẫu: **Input → Spark xử lý → Kết quả**.
Không gọi đây là IDS production hoặc live packet capture.

## Chuẩn bị trước giờ trình bày

Từ thư mục:

```powershell
D:\Learning\bigdata\SparkNIDS-Spark-Network-Intrusion-Detection-System
```

Chạy kiểm tra:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Mở hai terminal.

Terminal 1:

```powershell
.\scripts\run_demo.ps1
```

Terminal 2:

```powershell
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Mở `http://127.0.0.1:8501`. Khi runner còn chạy, mở thêm
`http://127.0.0.1:4040` để xem Spark UI.

## Kịch bản theo thời gian

| Thời gian | Mở trên dashboard | Câu nói chính | Bằng chứng cần chỉ |
|---:|---|---|---|
| 0:00–0:45 | Bước 1 · Dữ liệu và schema | Mỗi dòng là một network flow; mục tiêu là dự đoán Normal hay Attack. | Schema DataFrame, số flow, hai label |
| 0:45–1:45 | Bước 2 · CSV và Parquet | Spark đọc cùng dữ liệu bằng hai format, chạy cùng query và trả cùng kết quả. | Hash giống nhau, thời gian đọc, kích thước |
| 1:45–2:45 | Bước 3 · Lazy evaluation | Viết transformation chưa có nghĩa là Spark đã chạy; action mới tạo Job. | Job trước action bằng 0, sau action có Job |
| 2:45–4:15 | Bước 4 · Job/Stage/Task | Spark chia dữ liệu thành partition, chia công việc thành Stage và Task. | Spark UI Jobs/Stages, Task count, Exchange |
| 4:15–5:15 | Bước 5 · Cache | Cache lưu kết quả trung gian để dùng lại; lần đầu vẫn phải trả chi phí materialize. | Materialize, reuse timing, hòa vốn |
| 5:15–7:15 | Bước 6 · MLlib Random Forest | Pipeline biến cột mạng thành vector; Random Forest dự đoán Attack. | Pipeline stages, feature dimension, F1/Recall/ROC-AUC |
| 7:15–8:45 | Bước 7 · Structured Streaming | File mới được xử lý thành micro-batch; checkpoint giúp restart không đọc lại file cũ. | Batch ID, checkpoint, restart checks |
| 8:45–10:00 | Nhìn lại stepper | Spark cung cấp lớp xử lý dữ liệu lớn; model là một phần trong pipeline. | Giới hạn local/offline và hướng phát triển |

## Lời thoại ngắn từng bước

### 1. Dữ liệu và schema

“Đầu vào không phải packet sống mà là các network flow trong UNSW-NB15. Mỗi
dòng có thông tin protocol, service, byte, packet và label. Spark đọc thành
DataFrame có schema rõ ràng để các bước sau không phải đoán kiểu dữ liệu.”

### 2. CSV và Parquet

“CSV dễ nhìn với con người nhưng là text. Parquet lưu theo cột và có schema cho
máy. Ở đây Spark chạy đúng cùng một aggregate trên hai format; hash kết quả
giống nhau nên ta đang so chi phí đọc, không so hai phép tính khác nhau. Số
speedup chỉ có ý nghĩa trên máy và dữ liệu hiện tại.”

### 3. Lazy evaluation

“Các lệnh filter, groupBy, orderBy chưa lập tức chạy. Spark đang dựng kế hoạch.
Khi gọi collect là action, Spark mới tối ưu kế hoạch và tạo Job. Đây là lý do
Spark có thể nhìn toàn bộ chuỗi xử lý trước khi thực thi.”

### 4. Job, Stage, Task, Shuffle

“Một Job là yêu cầu do action tạo ra. Stage là các đoạn Spark có thể chạy liên
tiếp. Mỗi partition tạo ra Task. groupBy/orderBy cần gom dữ liệu giữa các
partition nên xuất hiện Exchange, tức shuffle. Ta có thể đối chiếu con số này
trực tiếp ở Spark UI.”

### 5. Cache

“Cache không làm mọi lệnh tự nhiên nhanh hơn. Spark phải tính và lưu cache ở
lần đầu. Nếu DataFrame được dùng lại đủ nhiều, các action sau giảm chi phí đọc
và tính lại. Demo cố tình tách hai loại thời gian để tránh hiểu nhầm.”

### 6. MLlib Random Forest

“Spark MLlib gom StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
và Random Forest vào Pipeline. Demo train trên sample modeling để nhanh và dễ
quan sát. Official test không bị dùng lại; kết quả chính thức của đồ án nằm ở
artifact Phase 6/7.”

### 7. Structured Streaming

“Ta mô phỏng luồng dữ liệu bằng các file Parquet đến từng đợt. foreachBatch dùng
model đã khóa để ghi prediction. Sau khi dừng query, file mới được thêm vào.
Cùng checkpoint giúp Spark biết file cũ đã xử lý, vì vậy lượt restart chỉ nhận
file mới.”

## Câu trả lời ngắn cho câu hỏi thường gặp

- **Spark đóng góp gì?** DataFrame/schema, Spark SQL, lazy execution,
  partition/task/stage, shuffle, cache, Parquet, MLlib và Structured Streaming.
- **Có phải real-time không?** Chưa. Đây là file-source micro-batch; Kafka hoặc
  network-flow collector là hướng phát triển tiếp.
- **Random Forest là gì?** Nhiều cây quyết định bỏ phiếu cùng nhau; thường ổn
  định hơn một cây đơn trong bài toán phân loại.
- **Threshold 0,60 là gì?** Xác suất Attack từ 0 đến 1; nếu từ 0,60 trở lên
  thì hệ thống gắn nhãn Attack. Threshold được chọn trên validation, không chọn
  lại theo official test.
- **Parquet luôn nhanh hơn CSV không?** Không. Demo chỉ chứng minh số đo trên
  workload hiện tại; query, kích thước, cache, disk và số core đều ảnh hưởng.
- **Nếu FPR cao thì sao?** Đây là mô hình nghiên cứu; cần calibration, dữ liệu
  mới, phân tích chi phí FP/FN và đánh giá production trước khi chặn traffic.

## Phương án dự phòng

| Sự cố | Cách xử lý | Không được làm |
|---|---|---|
| Runner Spark không khởi động | Dashboard tự dùng artifact offline trong `outputs/dashboard` và `outputs/metrics`. | Không bịa số liệu live. |
| Dashboard không mở | Mở JSON/report và dùng lời thoại trong tài liệu này. | Không chạy lại official test tại chỗ. |
| Spark UI không vào được | Tiếp tục bằng bằng chứng đã ghi trong status/physical plan. | Không khẳng định đã xem UI nếu chưa xem. |
| Model cuối bị thiếu | Bỏ qua streaming, nói rõ recovery `python -m src.finalize_model`. | Không train lại model cuối trong lúc thuyết trình. |
| Máy chậm | Chạy từng step với `--step`, giảm hold time, dùng fallback. | Không tăng dữ liệu tùy ý để lấy số đẹp. |

## Checklist nghiệm thu

- [ ] `pytest -q` pass.
- [ ] Runner chạy được `--step data` và tạo `outputs/demo/latest.json`.
- [ ] Dashboard mở được ở `127.0.0.1:8501`.
- [ ] Có thể đi qua đủ 7 bước bằng nút `Tiếp theo`.
- [ ] Runner 7 bước có status riêng cho từng step.
- [ ] Spark UI mở được khi runner đang hold SparkSession.
- [ ] Nêu rõ local mode, benchmark có điều kiện, file-source và FPR cao.
- [ ] Có artifact fallback nếu live demo lỗi.
