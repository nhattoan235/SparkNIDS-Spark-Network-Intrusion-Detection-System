# Spark execution và benchmark — Phase 8

Sinh lúc: `2026-09-10T21:27:01+07:00`.

## CSV so với Parquet

Hai định dạng chứa cùng 175.341 dòng Bronze, dùng cùng schema và truy vấn exact-integer. Sau warmup bằng nhau, thứ tự đo được đảo xen kẽ; mỗi định dạng chạy ba lần.

| Format | Dung lượng (MB) | Partitions đầu vào | Median (s) | Min (s) | Max (s) |
|---|---:|---:|---:|---:|---:|
| CSV | 30.797 | 4 | 0.3288 | 0.2973 | 0.4002 |
| PARQUET | 12.565 | 4 | 0.1981 | 0.1933 | 0.2101 |

Parquet nhanh hơn CSV theo median **1.66x** trong phép đo này; hash kết quả hai định dạng giống nhau.

## Cache khi tái sử dụng DataFrame

Materialize cache 175341 dòng mất `1.1041s` và không được tính vào reuse timing.

| Chế độ | Median (s) | Min (s) | Max (s) |
|---|---:|---:|---:|
| Không cache | 0.2167 | 0.1741 | 0.2356 |
| Có cache | 0.1867 | 0.1378 | 0.2920 |

Cache tăng tốc median mỗi action tái sử dụng **1,16x**, nhưng ba lần dùng chỉ
tốn `0,6264s` khi không cache so với `1,7206s` nếu tính cả materialize. Với
median quan sát được, ước tính cần khoảng **37** lần tái sử dụng để hòa vốn;
cache không mặc nhiên nhanh hơn.

## Số partition

Mỗi mức được repartition, cache và materialize trước; thời gian dưới đây chỉ đo aggregate trên các partition đã chuẩn bị.

| Partitions | Materialize (s) | Aggregate median (s) | Min (s) | Max (s) |
|---:|---:|---:|---:|---:|
| 1 | 1.2986 | 0.0664 | 0.0495 | 0.1261 |
| 2 | 0.8043 | 0.1407 | 0.1137 | 0.1689 |
| 4 | 0.7462 | 0.1266 | 0.1244 | 0.2305 |
| 8 | 0.8007 | 0.1781 | 0.1560 | 0.3404 |

Aggregate median tốt nhất trong lần chạy này là **1 partition**, còn
repartition/materialize nhanh nhất ở **4 partitions**. Kết quả chỉ áp dụng cho
local[2], dữ liệu và truy vấn aggregate nhỏ hiện tại; nhiều partition hơn core
có thể tăng overhead scheduler.

## Bằng chứng execution

Spark status tracker ghi **4 Job** và **8 stage ID** dưới Adaptive Query
Execution. Có 20 task được khai báo trên các stage, nhưng chỉ **7 task** thuộc
**4 stage** thực sự hoàn tất; các stage còn lại là phương án adaptive bị skip và
task thất bại bằng **0**. Physical-plan text chứa **4** lần xuất hiện `Exchange`
trên cả initial/final adaptive plan, thể hiện shuffle do `groupBy`/`orderBy`.

```text
Action collect()
  -> Job
     -> Stage trước Exchange: đọc từng input partition -> một Task/partition
     -> Exchange (shuffle theo label, proto)
     -> Stage aggregate: một Task/shuffle partition
     -> Exchange/orderBy -> Stage kết quả

local[2] -> một JVM local executor -> tối đa 2 Task chạy đồng thời trên 2 task slots
```

Transformation như `filter`, `groupBy`, `orderBy` là lazy; action `collect()` mới tạo Job. Spark tách Job thành Stage tại wide dependency/Exchange, và mỗi partition của Stage tạo một Task. File plan đầy đủ được lưu riêng để đối chiếu.

## Vì sao dùng Spark

Tập development 175 nghìn dòng vẫn có thể xử lý bằng Pandas, nên benchmark local này không được dùng để tuyên bố Spark luôn nhanh hơn. Giá trị của Spark ở đồ án là một pipeline thống nhất có schema, lazy execution, partition, cache, shuffle, Parquet pruning và MLlib, có thể mở rộng sang bộ UNSW-NB15 đầy đủ khoảng 2,54 triệu dòng hoặc cluster mà không viết lại thuật toán chính.

## Giới hạn benchmark

- Chạy local trên một máy; không đo network shuffle hoặc nhiều executor.
- Ba lần đo sau warmup giảm nhiễu nhưng không loại bỏ hoàn toàn OS filesystem cache và JVM JIT.
- Kết quả partition phụ thuộc truy vấn, kích thước dữ liệu, số core và cấu hình Spark; không phải quy tắc chung.
- Chỉ collect bảng aggregate nhỏ; không đưa toàn bộ dataset về driver.
