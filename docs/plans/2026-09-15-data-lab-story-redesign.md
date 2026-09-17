# Thiết kế lại dashboard: Data Lab Story

## Mục tiêu đã thống nhất

- Demo dành cho người chưa hiểu sâu Apache Spark.
- Spark là nội dung chính; IDS là bài toán minh họa.
- Người xem phải biết đang xem dữ liệu gì, Spark làm gì và kết quả nói lên điều gì.
- Giữ 7 bước, gom dưới 3 giai đoạn để dễ định hướng.
- Ưu tiên bảng dữ liệu và câu giải thích ngắn; hạn chế card KPI và chữ kỹ thuật.
- Chạy local bằng Streamlit, dùng artifact thật do Spark tạo và có thể mở rộng thành đồ án IDS.

## Giả định

- Một người trình bày trên laptop, không cần tài khoản hay triển khai cloud.
- Dữ liệu demo không chứa thông tin riêng tư.
- Ưu tiên rõ ràng và ổn định hơn hiệu ứng chuyển động.
- Tiếp tục dùng Python, Streamlit và hợp đồng artifact hiện tại.

## Hướng thiết kế

Tên phong cách: **Data Engineering Lab**.

Ba giai đoạn:

1. **Chuẩn bị dữ liệu**: Schema, CSV/Parquet, Lazy Evaluation.
2. **Spark xử lý phân tán**: Job/Stage/Task, Cache.
3. **Phát hiện xâm nhập**: MLlib Random Forest, Structured Streaming.

Mỗi bước dùng cùng một trật tự:

1. Câu hỏi cần trả lời.
2. Bảng dữ liệu trước khi xử lý.
3. Spark đang làm gì, diễn đạt bằng tiếng Việt và tối đa ba dòng lệnh chính.
4. Bảng kết quả sau khi xử lý.
5. Điều vừa được chứng minh.
6. Liên hệ với hệ thống IDS.

## Nội dung bảy bước

| Bước | Câu hỏi ngắn | Minh chứng chính |
|---|---|---|
| Schema | Spark hiểu dữ liệu mạng thế nào? | Flow mẫu và bảng tên cột, kiểu, ý nghĩa |
| CSV/Parquet | Format nào giúp Spark đọc nhanh hơn? | Bảng các lần chạy và thời gian |
| Lazy Evaluation | Khi nào Spark thực sự chạy? | Timeline transformation đến action |
| Job/Stage/Task | Spark chia việc lớn thế nào? | Bảng Job, Stage, Task, trạng thái |
| Cache | Lưu vào RAM có nhanh hơn không? | Bảng lần đầu và lần chạy lại |
| Random Forest | Mô hình nhận ra Attack thế nào? | Dữ liệu mẫu, confusion matrix, chỉ số |
| Streaming | Flow mới được xử lý thế nào? | Bảng micro-batch và dự đoán |

## Quy tắc nội dung

- Mỗi khối chỉ một đến hai câu ngắn.
- Dùng tiếng Việt phổ thông trước, thuật ngữ Spark đặt sau trong ngoặc.
- Mọi con số đều có đơn vị và một câu hướng dẫn cách đọc.
- Không dùng câu chung chung như “Bằng chứng Spark”. Dùng “Spark thực sự đã chạy gì?”.
- Chi tiết sâu như physical plan đặt trong phần mở rộng.
- Không khẳng định Parquet hoặc cache luôn nhanh hơn; kết luận dựa trên lần chạy hiện tại.

## Bố cục

- Header gọn: tên demo và trạng thái lần chạy.
- Thanh ba giai đoạn luôn hiện rõ vị trí hiện tại.
- Tiêu đề bước là một câu hỏi.
- Khu vực trung tâm ưu tiên bảng đầu vào và bảng kết quả.
- Màu cam chỉ đánh dấu thao tác của Spark.
- Điều hướng đặt cuối màn hình: bước trước, chi tiết Spark, bước tiếp theo.

## Trạng thái và lỗi

- Chưa chạy: ghi “Dữ liệu minh họa từ lần chạy gần nhất”.
- Đang chạy: hiển thị bước hiện tại và cập nhật artifact.
- Bước lỗi: ghi nguyên nhân ngắn và cách chạy lại; vẫn cho xem các bước khác.
- Spark UI không hoạt động: vô hiệu hóa liên kết và giải thích Spark session đã dừng.

## Kiểm thử

- Kiểm tra đủ 3 giai đoạn và đúng thứ tự 7 bước.
- Mỗi bước phải có câu hỏi, phần Spark xử lý và kết luận.
- Kiểm tra bảng CSV/Parquet, Job/Stage/Task, Cache, MLlib và Streaming render được từ fixture.
- Kiểm tra trạng thái offline, running, failed và Spark UI không còn hoạt động.
- Kiểm tra thủ công ở màn hình desktop và chiều rộng nhỏ.

## Decision log

- Chọn Data Lab Story thay vì dashboard KPI hoặc trang cuộn dài vì phù hợp việc thuyết trình Spark.
- Giữ 7 bước để không mất nội dung kỹ thuật, nhưng gom thành 3 giai đoạn để giảm rối.
- Dùng từ “Giai đoạn”, không dùng “Chương”.
- Đưa bảng dữ liệu lên trước biểu đồ và metric.
- Dùng câu ngắn, thuật ngữ được giải thích ngay tại chỗ.
- Giữ chi tiết kỹ thuật sâu nhưng không đặt ở luồng đọc chính.

## Rủi ro đã nhận diện

- Artifact hiện tại có thể chưa chứa đủ dòng dữ liệu mẫu cho mọi bảng; khi thiếu sẽ dùng bảng tóm tắt trung thực từ số liệu hiện có.
- Streamlit giới hạn một số bố cục; ưu tiên HTML/CSS nhẹ và component chuẩn để giữ ổn định.
- Số đo hiệu năng thay đổi theo máy; giao diện phải ghi rõ đây là kết quả của lần chạy hiện tại.
