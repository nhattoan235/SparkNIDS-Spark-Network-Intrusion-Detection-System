# Demo tương tác và bài đo Spark IDS

## Demo cho người xem

Mở PowerShell ở thư mục dự án, bật Spark IDS:

    . .\scripts\project_env.ps1
    .\.venv\Scripts\streamlit.exe run dashboard\app.py --server.port 8501

Mở http://127.0.0.1:8501, chọn trang Live Lab. Docker Desktop cần chạy ở chế độ Linux containers.

Nếu đây là lần đầu, bấm “Chuẩn bị mô hình bằng Docker”. Hệ thống chạy 15 sender trong ba lượt Docker độc lập để tạo dữ liệu huấn luyện, chọn ngưỡng và kiểm tra cuối. Bước này mất vài phút; xem tiến độ trên trang. Có thể chuẩn bị trước bằng:

    . .\scripts\project_env.ps1
    .\.venv\Scripts\python.exe -m src.lab_window_model --collect --duration 50

Sau khi mô hình sẵn sàng, bấm “Chạy 15 máy cùng lúc”. Chế độ tự động bắt đầu bằng traffic thường, chuyển một số máy sang hành vi cần kiểm tra sau 15 giây và trở về bình thường sau 60 giây. Mỗi máy vẫn chạy traffic riêng; seed được lưu để tái hiện lịch tự động.

Để tương tác, chọn một máy, đổi hành vi và tốc độ, rồi bấm “Áp dụng cho máy đã chọn”. Có thể đưa máy về “Theo kịch bản”. Hành vi kiểm tra có quét các cổng mồi trong Docker, gửi dồn hoặc gửi dồn ngắt quãng. Traffic không rời mạng nội bộ của Compose. Mô phỏng dừng sau thời lượng chọn được hoặc bấm “Dừng gửi traffic”.

## Người xem đang nhìn gì?

| Trên màn hình | Có nghĩa là |
|---|---|
| Sơ đồ 15 máy → server → Spark → MLlib | Những bước đang xử lý lượt traffic |
| Kết nối máy chủ nhận | Số kết nối TCP victim đã thật sự ghi nhận |
| Flow Spark gom | Số flow mới mà Spark đã nhóm vào cửa sổ của máy |
| Tốc độ/giây, cổng mồi (%) | Số đo từ traffic; nhãn và kịch bản không đi vào mô hình |
| Điểm nghi ngờ | Xác suất Random Forest gán cho hành vi đáng ngờ |
| Tạm tính | Cửa sổ chưa nhận đủ dữ liệu đến muộn để đánh giá |
| Cửa sổ đã đối chiếu | Cửa sổ Spark đã khớp số flow với nhãn độc lập |
| Bắt đúng/báo nhầm/bỏ sót | Tổng kết trên mọi cửa sổ đã đối chiếu trong lượt |

Structured Streaming đọc các file JSONL mới, dùng event time cùng watermark 30 giây, rồi cập nhật đặc trưng 10 giây theo IP nguồn. Báo cáo tiến độ Spark hiện số flow của micro-batch, tốc độ Spark xử lý, thời gian batch, watermark và trạng thái Spark. Spark giữ trạng thái cửa sổ trong checkpoint; trang chỉ đọc snapshot dự đoán nhỏ và nhãn đối chiếu được ghi riêng.

Khi cả 15 sender hoàn tất, trang chốt các cửa sổ còn lại bằng cách kiểm tra số flow Spark gom có khớp số flow victim ghi nhãn hay không. Tổng Bắt đúng/Báo nhầm/Bỏ sót/Bình thường đúng tính mỗi cửa sổ đúng một lần; nó khác với số máy đang bị gắn cờ ở cửa sổ mới nhất.

Mô hình dùng ba số đo: tốc độ kết nối, tỷ lệ kết nối tới cổng mồi và tỷ lệ HTTP. Train, validation và test lấy từ ba experiment Docker riêng. Random Forest và luật đơn giản đều chấm cùng tập test để người xem thấy MLlib có cải thiện thực sự không. Kết quả mô tả bộ traffic kiểm soát này; không đại diện mọi mạng.

## Bài đo dữ liệu lớn

Mở mục “Bài đo dữ liệu lớn · UNSW-NB15” bên dưới mô phỏng, bấm “Tải dữ liệu đã kiểm chứng và chạy phép đo”. Lần đầu hệ thống tải các file CSV theo SHA-256 trong cấu hình, xác nhận dữ liệu và chuẩn bị Bronze/Silver Parquet. Sau đó cùng một truy vấn Spark chạy trên 5.000, 35.000 và toàn bộ dữ liệu train, với ba partition khác nhau ở quy mô lớn.

Bảng thể hiện số flow, dung lượng Parquet, Task, thời gian tạo cache và trung vị của ba lần đo. Kết quả tổng hợp có SHA-256 để xác nhận chia partition không làm đổi kết quả. Một lượt làm nóng chạy trước các lượt tính giờ. Phép đo dùng Silver Parquet đã cache; thời gian không tính tải, chuyển đổi dữ liệu hay ghi Parquet. Ứng dụng chạy local[2] trên máy cá nhân, nên số liệu cho thấy ảnh hưởng của quy mô đầu vào và partition trên máy này, không phải tốc độ của Spark cluster.

Có thể chạy phép đo ngoài giao diện:

    . .\scripts\project_env.ps1
    .\.venv\Scripts\python.exe -m src.lab_scale

## Giới hạn cần nói rõ

15 container là 15 nguồn traffic Docker, không phải 15 máy Spark. Server ghi thông tin kết nối TCP ở tầng ứng dụng; lab không thu PCAP. Các hành vi là tình huống được tạo có kiểm soát. Báo nhầm, bỏ sót và độ trễ đều là kết quả đo, không nên sửa hoặc làm tròn để trông đẹp hơn. Đóng tài nguyên Docker khi demo xong:

    docker compose -p sparknids-live-lab down

