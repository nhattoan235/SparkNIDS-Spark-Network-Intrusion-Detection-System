# Kịch bản demo pipeline UNSW-NB15 bằng Apache Spark

## Mục tiêu demo

Cho người xem thấy dữ liệu mạng quy mô lớn đi qua một quy trình phát hiện xâm nhập như thế nào:

**CSV UNSW-NB15 → Spark đọc và chuẩn hóa → Parquet và chia dữ liệu → Spark MLlib học → kiểm tra trên test độc lập.**

Demo này dùng bộ dữ liệu UNSW-NB15 đã thu thập sẵn, không bắt gói tin trực tiếp từ Internet. Spark đang chạy `local[2]` trên một laptop; đây là xử lý nhiều dữ liệu bằng Spark trên máy đơn, không phải cụm nhiều máy.

## Chuẩn bị trước khi trình bày

- Mở dashboard tại `http://127.0.0.1:8501/` và chọn trang **UNSW-NB15**.
- Xác nhận dữ liệu nằm trên ổ D, tại `D:\Learning\bigdata\SparkNIDS-Spark-Network-Intrusion-Detection-System\data\full_unsw`.
- Mở rộng **Dữ liệu, cách chia và bằng chứng Spark** để có thể chỉ cho người xem số dòng, cách chia tập và Job/Stage/Task.
- Không cần bật Docker cho demo này. Docker Live Lab là phần riêng, mô phỏng 15 máy gửi traffic theo thời gian thực.
- Lượt xử lý đầy đủ đã mất khoảng 4 phút trên máy này và tạo gần 1 GB kết quả. Nên chạy trước buổi trình bày. Chỉ bấm **Chạy pipeline UNSW đầy đủ** nếu muốn chạy lại từ đầu.

## Kịch bản gợi ý: 5–7 phút

| Thời gian | Thao tác trên web | Điều cần giải thích |
|---|---|---|
| 0:00–0:45 | Ở trang UNSW-NB15, chỉ vào tiêu đề và mô tả. | Đây là bài toán phân loại một lượt giao tiếp mạng thành bình thường hoặc tấn công. Mỗi dòng là một network flow, không phải một gói tin đơn lẻ. |
| 0:45–1:30 | Chỉ vào nguồn dữ liệu và số dòng. | Spark đọc 4 CSV, tổng khoảng 586 MB và hơn 2,54 triệu dòng. Đây là dữ liệu thật đã gắn nhãn, không phải số liệu vẽ minh họa. |
| 1:30–2:30 | Mở **Dữ liệu, cách chia và bằng chứng Spark**; chỉ Bronze/Silver, số dòng và các tập. | Spark kiểm tra dữ liệu, xử lý ô trống/không hợp lệ, lưu dữ liệu dạng Parquet rồi chia train, validation và test. Các dòng trùng được giữ, nhưng cùng fingerprint luôn nằm cùng một tập để tránh rò rỉ. |
| 2:30–3:30 | Chỉ vào model, số flow train và ngưỡng. | Spark MLlib học quy luật từ train. Validation dùng để chọn ngưỡng cảnh báo. Sau bước này, model và ngưỡng được khóa trước khi xem test. |
| 3:30–5:00 | Chỉ vào bảng kết quả và ma trận nhầm lẫn. | Spark chấm từng flow trong test độc lập. Giải thích số bắt đúng, bỏ sót và báo nhầm bằng số đếm trước, rồi mới nói đến phần trăm. |
| 5:00–6:00 | Chỉ vào Job/Stage/Task và Spark version/master. | Job là một công việc Spark được kích hoạt bởi thao tác cần kết quả; Stage là các chặng xử lý; Task là phần việc Spark chạy trên partition. `local[2]` nghĩa là Spark dùng tối đa 2 luồng xử lý cục bộ trên laptop này. |
| 6:00–7:00 | Nêu giới hạn và liên hệ Live Lab nếu còn thời gian. | UNSW chứng minh xử lý và đánh giá trên dữ liệu lớn. Live Lab là demo khác để thấy traffic Docker đi qua Structured Streaming; hai nhánh không dùng chung model. |

## Lời thoại mẫu

### Mở đầu

“Đề tài của nhóm em là phát hiện xâm nhập mạng bằng Apache Spark MLlib. Ở phần này, em dùng dữ liệu UNSW-NB15 gồm hơn 2,5 triệu lượt giao tiếp. Mục tiêu là cho thấy Spark xử lý dữ liệu và MLlib dự đoán Attack hay Normal như thế nào.”

### Khi giới thiệu dữ liệu và Spark

“Spark đọc bốn file CSV, kiểm tra dữ liệu rồi ghi các bước trung gian thành Parquet. Dữ liệu được chia thành ba phần: train để học, validation để chọn ngưỡng, và test độc lập để đánh giá cuối. Em không dùng test để chỉnh model.”

“Spark chạy trên laptop bằng `local[2]`, nên đây chưa phải hệ thống nhiều máy. Tuy vậy, dashboard ghi lại số dòng và dấu vết Job, Stage, Task thực tế của lượt chạy này; đây là bằng chứng Spark đã xử lý dữ liệu, không chỉ là biểu đồ minh họa.”

### Khi giới thiệu kết quả

“Trên 379.440 flow test, model bắt đúng 45.670 lượt tấn công, bỏ sót 1.871 và báo nhầm 2.390 lượt bình thường. Recall khoảng 96,1%, nghĩa là trong tập test này model bắt được khoảng 96 trên 100 lượt tấn công. Báo động giả khoảng 0,7% số lượt bình thường.”

“Các con số này là kết quả của đúng lượt chạy đang hiển thị. Đây là đánh giá trên bộ dữ liệu nghiên cứu, không có nghĩa model đã sẵn sàng tự động chặn traffic ngoài đời.”

## Kết quả của lượt chạy đã hoàn thành

- Mã lượt: `unsw-20261011-125035-59f095`.
- Spark: `4.2.0`, chế độ `local[2]`.
- Nguồn: 4 CSV, 586.377.596 byte; checksum đã xác minh.
- Số dòng đọc: 2.540.047. Trang UNSW ghi 2.540.044; pipeline báo rõ chênh lệch 3 dòng và giữ nguyên dữ liệu đã xác minh.
- Chia tập: train **1.780.176**, validation **380.431**, test **379.440**.
- Model: Spark MLlib Logistic Regression; ngưỡng được chọn trên validation là **0,55**.
- Test: bắt đúng **45.670**, bỏ sót **1.871**, báo nhầm **2.390**, nhận đúng bình thường **329.509**.
- Chỉ số: Precision **95,0%**, Recall **96,1%**, F1 **95,5%**, báo động giả **0,7%**.
- Thời gian đo trong lượt này: chuẩn bị dữ liệu khoảng **169 giây**, fit model khoảng **41 giây**, chấm test khoảng **9 giây**. Thời gian thay đổi theo máy và trạng thái hệ thống.

Báo cáo đầy đủ và ví dụ flow được lưu trong `data/full_unsw/runs/<run-id>/report.json` trên ổ D. Mỗi lần chạy tạo một lượt mới, không ghi đè lượt trước.

## Nếu muốn chạy trực tiếp trước lớp

1. Mở trang **UNSW-NB15**.
2. Bấm **Chạy pipeline UNSW đầy đủ**.
3. Trong lúc chờ, giải thích lần lượt: đọc/kiểm tra dữ liệu → ghi Parquet/chia tập → fit MLlib/chọn ngưỡng → chấm test.
4. Nếu muốn xem Spark UI, mở `http://127.0.0.1:4040` khi Spark còn chạy. Spark UI sẽ đóng sau khi tiến trình kết thúc.
5. Dashboard hiện cần nhấn **F5** để đọc trạng thái mới trong lúc chạy; đừng bấm chạy lần nữa khi lượt trước chưa xong.
6. Khi hoàn thành, chỉ số mới sẽ thay thế kết quả đang hiển thị; mở phần bằng chứng để xem số liệu của lượt mới.

## Câu hỏi thường gặp

**“Đây có phải traffic tấn công trực tiếp không?”**
Không. Đây là bộ network flow UNSW-NB15 đã thu thập và gắn nhãn. Traffic Docker trực tiếp trong máy chỉ nằm ở Live Lab riêng.

**“Tại sao cần ba tập dữ liệu?”**
Train dùng để học; validation dùng để chọn ngưỡng; test độc lập dùng để kiểm tra cuối. Tách như vậy giảm nguy cơ báo kết quả đẹp do model đã nhìn thấy đáp án.

**“Spark làm gì, còn MLlib làm gì?”**
Spark đọc, biến đổi, chia và tổng hợp dữ liệu phân tán. MLlib cung cấp thuật toán/pipeline dùng để huấn luyện và dự đoán.

**“Đã chứng minh chạy cluster chưa?”**
Chưa. Lượt này chạy trên một laptop bằng `local[2]`. Nó chứng minh pipeline xử lý toàn bộ bộ dữ liệu qua Spark, nhưng chưa phải benchmark so sánh cluster nhiều máy.

**“Có thể dùng để chặn tấn công thật không?”**
Chưa nên. Đây là demo/đồ án trên bộ dữ liệu có sẵn; triển khai thực tế cần thu thập flow an toàn, kiểm định thêm trên dữ liệu mới, theo dõi báo động giả và kiểm soát việc chặn.

## Liên hệ với Docker Live Lab

Live Lab ở `http://127.0.0.1:8501/Live_Lab` mô phỏng 15 máy Docker gửi kết nối đến máy chủ đích. Phần đó minh họa luồng gần thời gian thực: máy gửi → máy chủ ghi nhận → Spark Structured Streaming gom theo cửa sổ → Spark MLlib chấm điểm. Đây là demo trải nghiệm traffic, không thay thế kết quả full-data UNSW ở trên.

### Nếu mở Live Lab: giải thích 15 máy và dấu hiệu bất thường

Nói trước: “15 máy” là 15 chương trình gửi kết nối chạy trong Docker, không phải 15 máy Spark hay 15 máy thật. `Traffic` trong trang này là các kết nối gửi tới máy chủ lab; không phải nội dung gói tin lấy từ Internet.

Ở chế độ **Tự chạy 3 giai đoạn**, cả 15 máy gửi bình thường trong 15 giây đầu. Từ giây 15 đến giây 60, kịch bản gán 7 máy vào nhóm hành vi bất thường; 5 máy vẫn truy cập thường và 3 máy truy cập web nhanh hợp lệ. Sau giây 60, tất cả trở về bình thường. Mã xáo mặc định là `42`; đổi mã sẽ đổi máy nhận hành vi, không đổi số lượng từng kiểu.

Với mã `42`, có thể chỉ theo nhóm máy sau (Máy 01 tương ứng `sender-01`):

| Máy | Hành vi trong giai đoạn giữa | Nhãn kịch bản |
|---|---|---|
| 01, 12 | Gửi dồn tới web | Bất thường |
| 02 | Quét cổng mồi chậm | Bất thường |
| 04, 05, 10 | Quét nhiều cổng mồi | Bất thường |
| 11 | Gửi dồn ngắt quãng | Bất thường |
| 03, 06, 13 | Truy cập web nhanh hợp lệ | Bình thường |
| 07, 08, 09, 14, 15 | Truy cập thường | Bình thường |

Quét cổng chỉ thử các cổng mồi `9001–9012` bên trong mạng Docker. Truy cập thường gửi khoảng 0,7 kết nối/giây tới cổng web `8080`; truy cập nhanh hợp lệ khoảng 4/giây; quét nhiều cổng khoảng 3/giây; gửi dồn khoảng 14/giây. Quét chậm dùng tốc độ khoảng 0,7/giây; gửi dồn ngắt quãng gửi nhanh rồi nghỉ theo chu kỳ. Đây là hành vi mô phỏng an toàn, không phải khai thác lỗ hổng.

**Spark bắt bất thường bằng cách nào?** Spark Structured Streaming gom các flow theo máy trong cửa sổ 10 giây. MLlib Random Forest chỉ nhận ba số đo: số kết nối trung bình mỗi giây, tỷ lệ kết nối đi tới cổng khác `8080`, và tỷ lệ kết nối HTTP. Điểm tấn công từ 0 đến 1 đạt ngưỡng hiện tại `0,70` thì giao diện đánh dấu “nghi ngờ”. Mô hình không nhận tên hành vi hay nhãn đúng/sai làm đầu vào; các nhãn được giữ riêng để đối chiếu sau dự đoán.

Khi thuyết trình ở Live Lab, chỉ bảng trước: “Mỗi hàng là một máy trong cửa sổ 10 giây gần nhất; `Kết nối/10 giây` là số flow Spark gom được, `Kết nối/giây` bằng số đó chia 10, `Tỷ lệ cổng quét` cho biết có bao nhiêu flow đi vào cổng mồi, còn `MLlib dự đoán` và `Điểm tấn công` là kết quả model.” Sau đó chỉ phần **Kết quả phát hiện** bên dưới bảng: “Bắt đúng là hành vi bất thường được nhận ra; bỏ sót là bất thường bị đánh giá bình thường; báo nhầm là bình thường bị nghi ngờ; bình thường nhận đúng là hành vi bình thường được nhận ra.” Các số này đếm những cửa sổ đã đối chiếu, không phải số máy.

Giới hạn cần nói rõ: đây là phân loại các mẫu traffic được tạo trong lab. Model không đọc payload để nhận diện mã độc hay xác định tên một cuộc tấn công ngoài đời; kết quả không đại diện cho mọi mạng.

Nguồn dữ liệu, checksum và quy tắc xử lý được mô tả tại [`DATASET_SOURCE.md`](DATASET_SOURCE.md).
