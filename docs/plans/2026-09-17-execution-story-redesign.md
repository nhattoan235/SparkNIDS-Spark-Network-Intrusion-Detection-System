# Giải thích Job, Stage, Task bằng một truy vấn thật

## Hiểu đúng yêu cầu

- Người xem mới học Spark phải biết Job, Stage, Task **làm gì**, không chỉ biết số lượng.
- Dùng cùng một truy vấn trên dữ liệu mạng để nối thuật ngữ với việc thực tế.
- Giai đoạn 3 vẫn tồn tại vì đề tài là IDS bằng Spark MLlib, nhưng chỉ kể ứng dụng, không giảng lại cơ chế Spark.
- Giữ câu ngắn và các bảng có hướng dẫn đọc.
- Tiếp tục chạy local bằng Streamlit với artifact hiện có.

## Giả định và ràng buộc

- Một người thuyết trình trên laptop; không có tải nhiều người dùng.
- Không thêm nguồn dữ liệu hay thông tin riêng tư.
- UI phải dùng được khi Spark session đã dừng và đọc được artifact lần chạy gần nhất.
- Nội dung thuộc dashboard, nên người làm đồ án có thể bảo trì trong Python.

## Các hướng đã cân nhắc

1. **Truy vấn thật + ba hàng giải thích** (chọn): gắn Job, Stage, Task với `filter → groupBy(label, proto) → count/sum → orderBy → collect`.
2. Chỉ dùng ví von: dễ nhớ nhưng thiếu bằng chứng Spark.
3. Chỉ mở Spark UI: nhiều thuật ngữ, không xem được khi Spark đã dừng.

## Thiết kế được chấp thuận

Giai đoạn 2, bước Job/Stage/Task:

- Nói bài toán trong một câu: đếm flow Normal/Attack theo giao thức và cộng byte gửi/nhận.
- Bảng ba hàng: thuật ngữ, nghĩa đơn giản, việc trong truy vấn. Job là đợt Spark thực thi; Stage là chặng giữa các lần chuyển dữ liệu; Task là việc trên một partition trong một stage.
- Cho thấy `groupBy` có thể cần chuyển các flow cùng nhóm về nơi tính tổng (shuffle).
- Bảng số liệu thật: 4 Job, 8 Stage, 20 Task khai báo, 7 Task hoàn tất theo bản ghi status tracker của lần chạy hiện tại, 0 Task lỗi. Không gọi 20 Task “đã hoàn tất”.
- Nói rõ một action có thể phát sinh nhiều Job; không viết quy tắc một-một.
- Chi tiết Job ID/Stage ID vẫn là phần mở rộng, không chen vào luồng chính.

Giai đoạn 3:

- MLlib: tập train để học, validation để kiểm tra; kết quả phân loại Normal/Attack, báo nhầm và bỏ sót. Ghi rõ đây là mô hình mẫu để dạy, không phải số kiểm thử cuối cùng của đồ án.
- Streaming: ba file Parquet được đưa lần lượt vào để **mô phỏng** luồng mới; mỗi micro-batch dự đoán Normal/Attack, checkpoint giúp không đọc lại file cũ. Không mô tả đây là bắt gói tin mạng trực tiếp.
- Mỗi bước chỉ thêm một câu “làm gì” và một câu “kết quả có ý nghĩa gì”.

## Rủi ro và cách xử lý

- `statusTracker` có thể ghi ít task hoàn tất hơn số task khai báo: ghi nhãn riêng và không diễn giải là lỗi khi Spark query đã thành công.
- Artifact không chứa các hàng aggregate theo giao thức: không dựng số giả. Chỉ hiển thị tổng số nhóm kết quả đã ghi và số đo thực thi hiện có.
- Runner cũ/fallback thiếu trường: hiển thị “Chưa có số liệu” thay vì số 0 giả.

## Decision log

- Chọn truy vấn thật thay vì ví von hoặc Spark UI-only để vừa dễ hiểu vừa kiểm chứng được.
- Giữ Giai đoạn 3 ngắn thay vì xóa để bảo toàn liên hệ với tên đề tài.
- Tách task khai báo và task hoàn tất thay vì gộp thành một KPI gây hiểu sai.
- Không thêm dữ liệu mẫu giả vào bảng kết quả.
