# Thiết kế demo IDS trên toàn bộ UNSW-NB15

## Mục tiêu và phạm vi đã chốt

- Đồ án: **Xây dựng hệ thống phát hiện xâm nhập mạng trên dữ liệu lớn bằng Apache Spark MLlib**.
- Nhánh chính xử lý bốn CSV gốc UNSW-NB15, [2.540.044 bản ghi theo UNSW](https://research.unsw.edu.au/projects/unsw-nb15-dataset), dùng Spark để chuẩn bị dữ liệu, huấn luyện MLlib và dự đoán trên test độc lập.
- Nhánh Docker 15 máy vẫn là minh họa lưu lượng trực tiếp, dùng mô hình lab riêng; không đánh đồng nó với mô hình UNSW.
- Chạy được trên laptop 16 GB RAM, Spark local[2]; không tuyên bố đã triển khai cluster hoặc Spark nhanh hơn mọi công cụ khác.
- Dữ liệu, Parquet, mô hình và kết quả lớn nằm trên ổ D; code hiện có được giữ nguyên vị trí cho tới khi người dùng yêu cầu chuyển. Không dùng Git nếu chưa được cho phép.

## Trải nghiệm người xem

Trang đầu là một đường đi rõ ràng: **Dữ liệu mạng → Spark xử lý → MLlib dự đoán → Đối chiếu kết quả**. Mỗi chặng có một câu mô tả và số liệu thật. Nút chính "Chạy phát hiện trên dữ liệu chưa dùng để học" chỉ mở khi có mô hình đã khóa và tập test hợp lệ. Trang cập nhật trạng thái, số dòng đã xử lý khi có nguồn đo thật, thời gian thực tế và lỗi có hướng khắc phục. Không đặt biểu đồ tốc độ hoặc thuật ngữ Spark làm nội dung trung tâm.

Kết quả cho xem một flow bắt đúng, một báo nhầm hoặc bỏ sót nếu có, với đặc trưng đầu vào, xác suất, ngưỡng, dự đoán và nhãn thật. Các đặc trưng được gọi là *dữ liệu quan sát*, không được trình bày như lời giải thích nhân quả của một dự đoán Random Forest. Mục "Spark đã làm gì?" mở rộng ra để thấy số partition, Job/Stage/Task, thời gian và phiên bản Spark. Kết quả được đánh dấu rõ là **lượt chạy lại trên tập nào** hay **artifact đã chuẩn bị**; không hiển thị tiến độ hoặc thời gian còn lại giả.

Live Lab dùng chung hệ màu, cách đặt tên và trình tự bốn chặng, nhưng có nhãn nổi bật "mô hình Docker riêng". Phần học thuật Guided Demo cũ được giữ ở khu vực thứ cấp, không chen vào đường đi chính.

## Dữ liệu và chống rò rỉ

1. Ghi nguồn, giấy phép học thuật, dung lượng và SHA-256 thực tế của từng CSV. Nếu phải dùng mirror, ghi là mirror; không tự nhận file đã được UNSW xác thực khi chưa có checksum công bố.
2. Kiểm tra cấu trúc bốn file, kiểu dữ liệu, nhãn, dòng lỗi, trùng lặp và khả năng ánh xạ sang tập đặc trưng thống nhất. Dừng với báo cáo rõ ràng nếu cấu trúc không phù hợp; không âm thầm bỏ dòng.
3. Tạo Bronze/Silver Parquet bằng Spark; báo số dòng gốc, hợp lệ, trùng và bị loại. Gán flow vào train/validation/test bằng khóa ổn định sau chuẩn hóa, bảo đảm cùng một flow/nhóm trùng không nằm ở nhiều tập. Công bố số dòng và tỷ lệ từng lớp thực tế.
4. Không đưa `label`, `attack_cat`, định danh hoặc metadata nguồn vào đặc trưng mô hình. Chỉ fit bộ biến đổi và mô hình trên train; chọn mô hình/ngưỡng trên validation; khóa lại trước khi xem test. Hai file designated train/test cũ là thí nghiệm khác, không dùng test cũ để chấm mô hình học từ bốn file gốc vì có nguy cơ giao nhau.

## Mô hình, tài nguyên và kiểm chứng

Mô hình chính cho bộ đầy đủ là Spark MLlib Logistic Regression với tập đặc trưng đã kiểm tra và số vòng lặp hữu hạn: đây là phương án ưu tiên khả năng chạy hết trên laptop. Random Forest hiện có vẫn được dùng ở Live Lab và có thể được so sánh trên thí nghiệm nhỏ riêng; chỉ quảng bá Random Forest toàn bộ nếu phép chạy toàn bộ thật sự hoàn tất. Trước lượt dài, thử 100 nghìn, 500 nghìn rồi 1 triệu dòng để ước lượng RAM/đĩa/thời gian. Bài làm **không đạt mốc bộ đầy đủ** nếu chỉ fit mô hình trên mẫu nhỏ; phải báo quy mô thật đã fit.

Tiêu chí nghiệm thu: bốn file gốc được kiểm kê; tổng dòng/loại bỏ/split khớp; tập test độc lập; MLlib fit thành công trên toàn bộ train đã công bố; confusion matrix và Precision/Recall/F1/FPR được tính từ test; nạp lại model cho cùng dự đoán; Job/Stage/Task và thời gian lấy từ lượt Spark thật; dashboard chỉ đọc artifact nhỏ, không gom hàng triệu dòng vào Streamlit. Kết quả lỗi cũng được lưu, không thay bằng số giả.

## Vận hành và rủi ro

- Huấn luyện, đánh giá toàn test và xuất ví dụ được chạy trước buổi thuyết trình. Nút chạy lại trên toàn test chỉ dùng nếu đã đo thời gian phù hợp; nếu không, nút chạy lại trên mẫu test có ghi rõ quy mô và hiển thị kết quả toàn test từ artifact đã khóa.
- Nếu tải file gốc bị chặn hoặc ổ D thiếu chỗ, dừng tại bước kiểm kê và báo điều kiện còn thiếu. Không nhân bản tập nhỏ thành "2,54 triệu".
- Nếu Logistic Regression toàn train không hoàn tất trong giới hạn máy, giảm độ rộng đặc trưng/cấu hình sau một phép đo có ghi nhận rồi chạy lại từ đầu; nếu vẫn không xong, công bố kết quả bán phần, không gọi là đã chứng minh trọn vẹn.
- Đường dẫn xuất phải được kiểm tra để tránh ghi đè artifact cũ; lần chạy mới có ID riêng, cập nhật con trỏ `latest` chỉ khi hoàn tất. Lab Docker chỉ hoạt động trong mạng Compose cô lập. Một người bảo trì dự án và tài liệu chạy lại phải đủ để tái hiện kết quả.

## Nhật ký quyết định

| Quyết định | Phương án khác | Lý do |
|---|---|---|
| Dùng bốn CSV gốc làm nhánh chính | Chỉ dùng 257 nghìn dòng designated | Chứng minh quy mô mạnh hơn và gắn quy mô với MLlib. |
| Huấn luyện và đánh giá trên split mới của bộ đầy đủ | Train cả bốn file rồi chấm designated test | Tránh giao nhau train/test. |
| Ưu tiên Logistic Regression toàn bộ train | Ép Random Forest lớn hoặc chỉ học trên mẫu | Phù hợp laptop; vẫn là MLlib, số dòng học được phải là thật. |
| Huấn luyện trước, demo dự đoán và điều tra lỗi | Huấn luyện lâu ngay trước lớp | Người xem thấy kết quả và vai trò Spark mà không chờ vô hạn. |
| Tách mô hình Docker và UNSW | Nói cả hai là một mô hình | Hai nguồn có đặc trưng khác nhau. |
| Giao diện theo luồng bốn chặng | Bảng/biểu đồ benchmark làm trang chính | Người xem hiểu công việc của hệ thống trong vài giây. |
| Dữ liệu lớn ở D, không dùng Git | Giữ artifact lớn trên C hoặc commit | Tuân thủ ràng buộc người dùng và tránh đầy ổ hệ thống. |
