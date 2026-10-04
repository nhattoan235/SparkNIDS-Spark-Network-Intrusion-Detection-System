# Live Lab: 15 máy gửi traffic qua Spark IDS

Live Lab tạo các kết nối TCP trong một mạng Docker nội bộ. Victim ghi mỗi kết nối thành một flow JSON; Spark Structured Streaming gom flow theo từng máy gửi; mô hình Random Forest trong Spark MLlib chấm điểm từng lượt traffic.

## Luồng demo

1. Mở trang **Live Lab** và chọn **Tự động** hoặc **Tự chọn**.
2. Kiểm tra bảng phân công đủ 15 máy. Chế độ tự động chia 5 bình thường, 5 quét cổng, 5 gửi dồn; chế độ tự chọn yêu cầu tổng đúng 15 và ít nhất 1 máy bình thường.
3. Bấm **Chạy mô phỏng 15 máy**. Dashboard chuẩn bị victim và khởi chạy 15 container riêng.
4. Mỗi container báo sẵn sàng rồi chờ. Khi đủ cả 15 máy, dashboard phát tín hiệu bắt đầu chung.
5. Victim ghi traffic; Spark đọc các flow mới, gom theo máy và chấm bằng MLlib.
6. Bảng kết quả cập nhật số flow gửi/nhận, kết quả Spark, điểm nghi ngờ và đối chiếu bắt đúng/báo nhầm/bỏ sót.

Các kịch bản có kích thước cố định để dễ so sánh:

| Kịch bản | Traffic tạo ra |
|---|---:|
| Bình thường | 12 yêu cầu HTTP rải cách nhau |
| Quét cổng | 12 kết nối tới các cổng mồi 9001–9012 |
| Gửi dồn | 30 yêu cầu HTTP gửi đồng thời theo từng nhóm nhỏ |

Chế độ tự động xáo thứ tự máy bằng seed hiển thị trên trang. Seed cùng với bảng phân công được lưu trong hồ sơ lượt chạy. Mỗi lần chạy có mã riêng; nhãn thật được lưu ngoài dữ liệu đầu vào của mô hình.

## Chuẩn bị mô hình một lần

Mở PowerShell tại thư mục gốc dự án. Docker Desktop cần chạy ở chế độ Linux containers.

Khởi động máy nhận traffic:

    docker compose -p sparknids-live-lab -f lab/compose.yaml up --build -d victim

Tạo dữ liệu hiệu chỉnh theo đúng ba kịch bản của Live Lab. Dữ liệu mới được lưu riêng, không ghi đè bộ traffic cũ:

    docker compose -p sparknids-live-lab -f lab/compose.yaml run --rm generator training --lab-mode calibration --runs-per-scenario 60

Huấn luyện riêng mô hình Docker của Live Lab (không cần đọc lại Parquet UNSW-NB15):

    . .\scripts\project_env.ps1
    .\.venv\Scripts\python.exe -m src.lab_model --session-only

Khi có ít nhất năm lượt hoàn chỉnh của mỗi lớp, lệnh chọn riêng dữ liệu calibration mới; nếu chưa đủ, nó dùng lại bộ training cũ. Báo cáo train/validation/test ghi rõ nguồn dữ liệu. Trang chỉ bật nút mô phỏng khi có mô hình.

### Kiểm tra mô hình với traffic khác cấu hình

Bộ test calibration giữ riêng các lượt chạy, nhưng vẫn dùng đúng ba mẫu cố định của Live Lab. Vì vậy 100% trên bộ này chỉ cho thấy mô hình nhận ra các mẫu quen. Để kiểm tra khi số kết nối và nhịp gửi thay đổi, thu thêm các lượt Docker ở chế độ `training` (bộ này không được dùng để huấn luyện mô hình calibration):

    docker compose -p sparknids-live-lab -f lab/compose.yaml run --rm generator training --lab-mode training --runs-per-scenario 30

Chấm bộ khác cấu hình bằng mô hình đã lưu, không huấn luyện lại:

    . .\scripts\project_env.ps1
    .\.venv\Scripts\python.exe -m src.lab_model --audit-only

Trang Live Lab đặt hai bài kiểm tra cạnh nhau: số lượt đúng trên mẫu quen và số lượt đúng trên traffic khác cấu hình, kèm số báo động nhầm trong các lượt bình thường. Nếu huấn luyện lại với `--session-only`, báo cáo khác cấu hình cũng được tính lại khi có dữ liệu tương ứng. Cả hai bộ vẫn là traffic trong mạng Docker nội bộ; không suy ra độ chính xác trên mạng thực.

## Chạy demo

Mở PowerShell thứ nhất và chạy Spark Structured Streaming:

    . .\scripts\project_env.ps1
    .\.venv\Scripts\python.exe -m src.lab_stream

Mở PowerShell thứ hai để bật giao diện:

    . .\scripts\project_env.ps1
    .\.venv\Scripts\streamlit.exe run dashboard\app.py --server.port 8501

Mở http://127.0.0.1:8501, chọn **Live Lab**, chọn cách chia máy rồi bấm nút chạy. Khi 15 máy đã sẵn sàng, chúng cùng gửi traffic. Không cần chạy generator từng lệnh riêng.

Nếu dashboard hiện ít lượt Spark đã chấm, kiểm tra cửa sổ Spark có đang chạy lệnh src.lab_stream không. Có thể dừng lượt đang gửi bằng nút **Dừng lượt mô phỏng**.

## Đọc bảng kết quả

- **Máy gửi nhận:** số yêu cầu generator gửi thành công tới victim, trên tổng dự kiến.
- **Spark gom:** số flow Spark đã đọc cho máy đó, trên tổng dự kiến. Chờ đủ số này trước khi đánh giá.
- **MLlib dự đoán:** kết luận của mô hình dựa trên các đặc trưng traffic.
- **Điểm nghi ngờ:** xác suất mô hình gán cho lớp đáng ngờ; ngưỡng đã học quyết định nhãn cuối.
- **Đối chiếu:** so sánh dự đoán MLlib với kịch bản thực tế được giữ riêng.

Bắt đúng nghĩa là máy quét/gửi dồn được nhận ra. Bỏ sót nghĩa là traffic đáng ngờ bị gọi bình thường. Báo động nhầm nghĩa là máy bình thường bị đánh dấu đáng ngờ. Đây là kết quả của lượt demo hiện tại, khác với Precision/Recall/F1 trong tập test huấn luyện.

Lượt 15 máy mặc định dùng ba mẫu cố định để trình bày luồng Docker → Spark → MLlib. Đọc phần **Bài kiểm tra mô hình: mẫu quen và mẫu khác** bên dưới bảng nếu muốn đánh giá khả năng chịu thay đổi của mô hình. Phần trăm không được tạo ngẫu nhiên; mỗi con số tính từ dự đoán thật và nhãn đối chiếu của từng lượt đã ghi.

## Spark đang làm gì?

- **Structured Streaming:** theo dõi thư mục flow demo và đọc file mới theo micro-batch.
- **Spark SQL:** nhóm theo run_id, đếm flow/cổng và tính tốc độ, byte trung bình, thời lượng, tỷ lệ HTTP.
- **Spark MLlib:** đưa các đặc trưng của cả lượt vào Random Forest để chấm điểm.
- **Dashboard:** ghép nhãn thật sau khi dự đoán để trình bày kết quả. Nhãn không được đưa vào mô hình.

Dù có 15 container gửi đồng thời, Spark hiện chạy local[2] trên máy này, không phải cụm 15 máy Spark. Mục tiêu của 15 sender là tạo nhiều nguồn traffic cùng lúc; đây không phải phép đo hiệu năng cluster.

## Giới hạn an toàn và độ tin cậy

Mạng Compose đặt internal: true, generator chỉ kết nối victim trong mạng đó; kịch bản không tấn công Internet hoặc máy bên ngoài. “Quét cổng” chỉ chạm các cổng mồi của victim trong lab. Traffic do chương trình tạo có kiểm soát, không đại diện toàn bộ tấn công thực tế. Victim đo byte ở tầng ứng dụng, không thu PCAP hoặc byte Ethernet/IP.

## Dừng và xem log

Nút dừng chỉ dừng các container gửi traffic; Spark và victim có thể tiếp tục chạy để xem kết quả. Log riêng từng sender được lưu trong outputs/live_lab/actors. Tắt các dịch vụ Docker khi hoàn tất:

    docker compose -p sparknids-live-lab -f lab/compose.yaml down

