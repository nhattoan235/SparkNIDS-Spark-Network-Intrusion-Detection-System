# Full UNSW-NB15 IDS Demo Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Chứng minh trọn vẹn trên laptop rằng Spark xử lý bộ UNSW-NB15 gốc 2.540.044 flow, Spark MLlib học trên phần train của bộ đó, rồi phát hiện xâm nhập trên test độc lập bằng một giao diện người xem hiểu ngay.

**Architecture:** Một pipeline batch mới cho bốn CSV gốc, tách biệt với pipeline designated train/test và mô hình Docker hiện có. Spark ghi Bronze/Silver, split cố định, model MLlib, dự đoán Parquet và JSON tổng hợp nhỏ vào ổ D; Streamlit chỉ đọc JSON nhỏ và có thể yêu cầu chạy lại inference đã giới hạn. Không thêm database, Kafka, cluster hay thư viện giao diện mới.

**Tech Stack:** Python, PySpark SQL/MLlib 4.2, Parquet, Streamlit, Docker Compose hiện có, PowerShell, pytest.

---

## Quy tắc thực hiện

- Thiết kế được duyệt ở `docs/plans/2026-10-11-full-unsw-ids-design.md`. Các quyết định trong đó là hợp đồng; không trộn test designated cũ với model full-data.
- Không chạy lệnh Git, không tạo worktree/commit/push. Kỹ năng lập kế hoạch thường gợi ý commit theo bước, nhưng yêu cầu "không dùng Git khi chưa được phép" của người dùng được ưu tiên.
- Theo `@test-driven-development` khi bắt đầu code: mỗi thay đổi logic có một test thất bại trước, sửa nhỏ nhất rồi test xanh. Nếu gặp lỗi ngoài dự kiến, dùng `@systematic-debugging`. Giữ giao diện theo `@ui-ux-designer` và số file/phụ thuộc tối thiểu theo `@ponytail`.
- Không tải hoặc ghi đè dữ liệu cũ trong lượt lập kế hoạch. Khi thực hiện, kiểm tra chính xác thư mục đích trên D trước mọi thao tác thay thế. Chỉ xuất bản `latest` sau khi artifact mới đầy đủ; giữ lượt cũ để dự phòng thuyết trình.
- Mỗi mốc có điều kiện dừng. Không gọi demo "đã xử lý toàn bộ" nếu một trong bốn file chưa đọc, mô hình chỉ fit trên mẫu, hoặc test không độc lập.

## Bằng chứng cần xuất hiện trên màn hình

| Câu người xem hỏi | Bằng chứng từ lượt chạy thật |
|---|---|
| Dữ liệu nào? | Tên nguồn, bốn file, SHA-256, số dòng gốc/hợp lệ/trùng/bị loại, dung lượng CSV/Parquet. |
| Spark làm gì? | Số dòng chuẩn bị, partition, Job/Stage/Task, thời gian đọc/chuẩn bị/fit/score, Spark `local[2]`. |
| MLlib học từ bao nhiêu? | Số dòng train thật, model đã lưu, loại model, phiên bản/nguồn dữ liệu, thời gian fit. |
| Phát hiện được gì? | Confusion matrix: bắt đúng, báo nhầm, bỏ sót, bình thường đúng; Precision/Recall/F1/FPR. |
| Có kiểm tra độc lập không? | Split fingerprint không giao nhau, threshold khóa trước test, ví dụ test có nhãn thật và dự đoán. |

## Task 1 — Chốt nguồn và nơi lưu; dừng sớm nếu không có bốn file

**Files:** `configs/default.yaml` (thêm một mục `full_unsw`/đường dẫn duy nhất), `src/full_unsw.py` (CLI tối thiểu), `tests/test_full_unsw.py`, `docs/DATASET_SOURCE.md`.

1. Viết test thất bại cho `audit_sources(root)` khi thiếu một trong bốn tên `UNSW-NB15_1.csv`…`UNSW-NB15_4.csv`, sai đường dẫn hoặc file rỗng. Test đòi thông báo nêu đúng file thiếu, không tự tải/ghi đè.
2. Chạy `.\.venv\Scripts\python.exe -m pytest tests/test_full_unsw.py -q -p no:cacheprovider`; xác nhận test mới đỏ vì API chưa có.
3. Implement `audit_sources` bằng `pathlib` và `hashlib` theo luồng đọc khối, trả về tên, byte, SHA-256, provenance; chỉ đặt cấu hình root trên D ở một chỗ. Thêm kiểm tra dung lượng trống dựa trên kích thước file thực tế, không dùng con số ước tính cứng.
4. Chạy lại test; kỳ vọng xanh. Đặt bốn CSV vào thư mục D riêng sau khi xác minh nguồn truy cập. Nếu mirror được dùng, ghi URL + hash thực tế và gọi đúng là mirror, không gọi là file chính thức đã xác thực.
5. **Cổng dừng:** chưa có đủ bốn file, nguồn không rõ hoặc ổ D không đủ chỗ thì báo người dùng, không làm Task 2–8. Chưa có file gốc trong `data/raw` hiện tại; ở đó chỉ có designated train/test.

## Task 2 — Kiểm kê schema thật và dựng Bronze/Silver

**Files:** `src/full_unsw.py`, `src/unsw_nb15_schema.py` (chỉ mở rộng khi các cột thật sự dùng chung), `tests/test_full_unsw.py`, `docs/DATASET_SOURCE.md`.

1. Đọc vài dòng đầu và danh sách đặc trưng đi kèm bộ gốc, ghi bảng đối chiếu cột gốc → cột model. Xác nhận có/không header, số cột, nhãn, các giá trị thiếu và escape CSV trước khi chốt `StructType`. Không dùng schema 45 cột của designated split một cách mù quáng.
2. Viết fixture CSV nhỏ có dòng hợp lệ, thiếu cột, nhãn sai, numeric không đọc được và dòng trùng; test đòi audit đếm rõ từng loại. Nếu bản ghi hỏng, lưu quarantine/báo cáo; không im lặng lọc đi.
3. Implement đọc CSV bằng Spark với schema tường minh, thêm `source_file`, ghi Bronze trên D. Chuẩn hóa nhãn/giá trị thiếu và chọn các feature đã kiểm chứng, ghi Silver Parquet. Chỉ reuse `src/preprocess.py`/`src/features.py` khi hợp đồng cột phù hợp; nếu không, thêm mapping nhỏ trong `src/full_unsw.py`.
4. Chạy test nhỏ trước. Sau đó chạy audit trên cả bốn file, đối chiếu tổng dòng thô với **2.540.044** mà UNSW công bố; nếu khác, dừng và điều tra thay vì sửa kỳ vọng. Ghi rõ số dòng sau làm sạch/loại trùng, số bytes và checksum artifact.
5. **Cổng dừng:** nếu không xác định được nhãn hoặc mapping nhất quán, phải sửa thiết kế trước khi huấn luyện.

## Task 3 — Split độc lập, không rò rỉ nhãn

**Files:** `src/full_unsw.py`, `tests/test_full_unsw.py`.

1. Viết test với hai dòng cùng fingerprint ở hai file gốc và một cặp giống feature nhưng khác nhãn. API dự kiến `split_full_data(frame, seed=42)` phải đưa cùng fingerprint vào cùng một tập; cặp mâu thuẫn nhãn bị thống kê và xử lý theo quy tắc công bố.
2. Dùng fingerprint ổn định từ nội dung flow đã chuẩn hóa, **không chứa `label`, `attack_cat` hoặc tên file**. Tách theo hash cố định 70% train, 15% validation, 15% test sau khi xử lý trùng; không dùng `randomSplit` không kiểm chứng. Lưu từng split Parquet trên D và ID/run metadata.
3. Test đòi: tổng train+validation+test bằng Silver hợp lệ; giao ba tập bằng 0; phân phối nhãn được ghi; chạy với thứ tự file/partition khác vẫn có cùng phân bổ. Kiểm tra feature list không có nhãn, ID, IP/timestamp hay metadata nguồn nếu chúng có nguy cơ học thuộc môi trường.
4. Chạy Spark trên toàn bộ Silver và ghi audit thực tế. Nếu split làm mất hẳn một lớp ở validation/test hoặc fingerprint mâu thuẫn nhãn quá nhiều, dừng để xem lại quy tắc; không chấm model bằng tập đó.

## Task 4 — Chứng minh phương án chạy được trên laptop trước khi fit toàn bộ

**Files:** `src/full_unsw.py`, `configs/default.yaml`, `tests/test_full_unsw.py`.

1. Viết test không cần dataset lớn cho chọn mức pilot và metadata `input_rows`, `fit_rows`, thời gian, model type, Spark master; không được gán `fit_rows` bằng số dòng toàn bộ khi mới fit mẫu.
2. Dùng MLlib Logistic Regression làm model chính của nhánh full-data. Giữ feature set đủ giải thích, tránh biến high-cardinality/metadata không cần thiết; fit preprocessing **chỉ** trên train. Giới hạn `maxIter` và cấu hình Spark full-data riêng với lab; không chạy huấn luyện đồng thời với Live Lab.
3. Chạy thử tăng dần trên **bản ghi gốc khác nhau**: 100.000 → 500.000 → 1.000.000; đo RAM, ổ D, thời gian, số dòng fit thật. Không nhân bản dòng để tạo quy mô giả. Chỉ chọn cấu hình toàn bộ sau pilot; ghi lại trường hợp thất bại.
4. **Cổng dừng:** nếu pilot không ổn, thu gọn feature/chỉnh partition hoặc bộ nhớ driver trong giới hạn laptop rồi thử lại. Nếu vẫn không fit được toàn bộ train, dừng tuyên bố "trọn vẹn" và báo phạm vi thực tế. Random Forest toàn bộ chỉ là tùy chọn sau khi model chính hoàn tất, không chặn đồ án.

## Task 5 — Huấn luyện toàn train, khóa model rồi mới mở test

**Files:** `src/full_unsw.py`, `src/evaluate.py` (reuse, chỉ sửa nếu thiếu API), `tests/test_full_unsw.py`.

1. Viết test với toy dataset: train/validation/test được truyền riêng; test không được đọc khi fit hoặc chọn threshold; test chỉ được dùng sau khi metadata model và threshold đã khóa. Test nạp lại model cho cùng xác suất/dự đoán.
2. Fit một MLlib Pipeline trên **toàn bộ train split đã công bố**, tính threshold trên validation bằng `src.evaluate.threshold_metrics`/`select_threshold`. Ghi model, feature contract, source hashes, split counts, Spark version/master, thời gian fit và ngưỡng vào một run directory mới trên D.
3. Nạp lại model ở tiến trình scoring riêng. Score **toàn bộ test độc lập**, ghi Parquet dự đoán, confusion matrix và Precision/Recall/F1/FPR bằng `src.evaluate`. Không đổi model/ngưỡng sau khi thấy test. Không so metrics này như thể cùng protocol với designated test cũ.
4. Test nhỏ xanh, rồi chạy toàn bộ; kiểm tra `tp+fp+tn+fn == test_rows` và số dòng prediction bằng số dòng test. Nếu một bước lỗi, run ở trạng thái `failed`, không cập nhật con trỏ bản hoàn chỉnh.

## Task 6 — Xuất bằng chứng nhỏ, thật, có thể trình bày offline

**Files:** `src/full_unsw.py`, `dashboard/data_access.py`, `tests/test_full_unsw.py`, `tests/test_dashboard.py`.

1. Viết test cho manifest JSON gồm source, split, model, run ID, trạng thái, thời gian, metrics, bằng chứng Spark và mẫu flow giới hạn; test từ chối thiếu trường hoặc counts mâu thuẫn. Mẫu flow không quá vài chục dòng, có bắt đúng/báo nhầm/bỏ sót nếu thực tế có.
2. Reuse cách xuất aggregate giới hạn của `src/export_dashboard_data.py`; Spark chỉ collect tổng hợp và vài flow đã `limit`, tuyệt đối không `.toPandas()`/`.collect()` toàn bộ test. Lưu snapshot hoàn chỉnh và cập nhật `latest` theo cách nguyên tử.
3. Thu Job/Stage/Task và thời gian từ lượt Spark thật, nêu `local[2]`/một máy. Nếu không thu được trường nào thì hiển thị "chưa có số đo", không điền 0 hoặc số giả. Tính ETA chỉ từ các lượt đo có thật; nếu chưa đủ thông tin, dùng "đang xử lý".
4. Test trạng thái `missing`, `running`, `failed`, `ready`, artifact JSON lỗi, kết quả cũ và model không khớp nguồn. Trang phải cho biết bước khôi phục cụ thể.

## Task 7 — Làm lại trang chính thành một hành trình phát hiện xâm nhập

**Files:** `dashboard/app.py`, `dashboard/styles.py`, `dashboard/pages/01_Live_Lab.py` (chỉ sửa nhãn/liên kết), `dashboard/pages/02_Giai_Thich_Spark.py` (mỏng, giữ Guided Demo cũ nếu cần), `tests/test_dashboard.py`.

1. Viết AppTest thất bại cho bốn chặng **Dữ liệu → Spark → MLlib → Kết quả**, nguồn mô hình, số dòng train/test, một nút hành động có nhãn quy mô thật, ví dụ flow, và một khu vực Spark mở rộng. Test trang rỗng/lỗi có hướng dẫn, không tự khẳng định kết quả đã sẵn sàng.
2. Dùng lại token trong `dashboard/styles.py`, typography/spacing và cách báo trạng thái của Live Lab. Một CTA chính; không đưa 7 tab Guided Demo hoặc đồ thị tốc độ lên vùng đầu trang. Guided Demo cũ chuyển thành trang phụ để không mất nội dung giáo dục.
3. Hiển thị hai nhánh rõ ràng: "UNSW-NB15 · học trên dữ liệu lớn" và "Docker · thử lưu lượng trực tiếp, model riêng". Nút chạy lại test chỉ bật khi model đã khóa và đo đủ nhanh; nếu không, nhãn chính xác "Chạy lại N flow test" và toàn-test report hiện riêng là artifact đã chuẩn bị.
4. Chạy AppTest, kiểm tra bằng trình duyệt ở độ rộng laptop và điện thoại, bàn phím/contrast/khả năng đọc. Nhờ một người không đọc tài liệu trả lời trong vài giây: dữ liệu nào, Spark làm gì, dự đoán gì; nếu không trả lời được, sửa chữ và thứ tự thay vì thêm biểu đồ.

## Task 8 — Nút chạy lại inference và chống chạy trùng

**Files:** `dashboard/app.py`, `dashboard/full_unsw_control.py` (chỉ tạo nếu logic tiến trình không thể gọn trong app), `src/full_unsw.py`, `tests/test_full_unsw.py`, `tests/test_dashboard.py`.

1. Viết test: hai lần bấm nhanh không tạo hai Spark job; nếu tiến trình lỗi, trạng thái lỗi được lưu và nút cho phép chạy lại; model/test thiếu thì không khởi chạy.
2. Reuse mẫu điều khiển process/status của `dashboard/live_lab_controller.py`, nhưng không đưa logic Docker vào nhánh UNSW. Worker đọc model đã khóa, chấm tập test hoặc subset có tên/số dòng rõ, ghi run ID riêng; Streamlit chỉ poll JSON.
3. Đo một lần scoring toàn test. Nếu thời gian chấp nhận được cho lớp học, nút chạy toàn test; nếu chậm, chuẩn bị báo cáo toàn test trước và nút chạy subset chưa dùng để học. Cả hai đều phải có provenance và số dòng hiển thị.
4. Giữ trường hợp mất mạng/lỗi Docker không ảnh hưởng trang full-data. Không chạy full scoring và Live Lab Spark cùng lúc trên laptop, hoặc hiển thị cảnh báo/ngăn chạy chồng.

## Task 9 — Tài liệu, buổi diễn tập và nghiệm thu

**Files:** `README.md`, `docs/KICH_BAN_DEMO.md`, `docs/LIVE_LAB_GUIDE.md`, `docs/BAO_CAO_KY_THUAT.md`, `tests/test_dashboard.py` và các test mới.

1. Cập nhật tài liệu một kịch bản 5–8 phút: giới thiệu đề tài → chỉ nguồn/2,54 triệu → bấm dự đoán → mở ví dụ bắt đúng/báo nhầm → mở bằng chứng Spark → chuyển Live Lab → nói giới hạn `local[2]`. Viết câu trả lời ngắn cho "Sao 15 máy không phải big data?", "Mô hình nào dùng ở đâu?", "Có bị trùng train/test không?".
2. Chạy toàn bộ unit/integration test: `.\.venv\Scripts\python.exe -m pytest tests -q -p no:cacheprovider` với thư mục tạm riêng trên D nếu pytest cần ghi nhiều. Chạy lại pipeline từ đầu trên một thư mục D mới, kiểm tra SHA, counts, split, model, prediction, report, UI. Không dùng dữ liệu cũ để lấp số còn thiếu.
3. Diễn tập lúc có mạng và lúc offline; khởi động lại Streamlit; thử thiếu model/lỗi JSON/ổ D không đủ; kiểm tra artifact cũ vẫn xem được. Đọc màn hình như người mới: họ nhận ra hệ thống đang làm gì trong vài giây, không phải đọc tài liệu dài.
4. Đối chiếu mọi con số trên slide/README/giao diện với JSON/Parquet của **cùng run ID**. Ghi rõ giới hạn: một laptop, dữ liệu UNSW là dataset nghiên cứu, Docker là mô phỏng cô lập, không phải IDS production.

## Điều kiện hoàn tất

- Có nguồn bốn CSV và báo cáo kiểm kê thực tế; không nhân bản dữ liệu để tăng quy mô.
- Có model Spark MLlib được fit trên **toàn bộ phần train của bộ gốc sau làm sạch**, không chỉ mẫu pilot.
- Có test độc lập, không giao split; metrics và ví dụ flow xuất từ test đó; kết quả nạp lại model khớp.
- UI nói được bằng số thật: nguồn dữ liệu, Spark làm gì, mô hình nào, đầu ra nào, đúng/sai ra sao; không cần mở biểu đồ để hiểu nhiệm vụ.
- Có bằng chứng Spark thực sự chạy và có kịch bản 5–8 phút trên laptop; mọi giới hạn được nói đúng.
- Không dùng Git trong quá trình này nếu người dùng chưa cho phép lại.
