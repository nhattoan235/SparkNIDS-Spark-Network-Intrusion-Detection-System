# Data dictionary — UNSW-NB15 designated splits

Pipeline Phase 2 dùng đúng 45 cột có trong `UNSW_NB15_training-set.csv` và
`UNSW_NB15_testing-set.csv`. Kiểu dưới đây được khai báo bằng Spark `StructType`;
không suy luận bằng `inferSchema`.

| Nhóm | Cột | Kiểu Spark |
|---|---|---|
| Định danh | `id` | `int` |
| Phân loại lưu lượng | `proto`, `service`, `state` | `string` |
| Thời lượng/tốc độ | `dur`, `rate`, `sload`, `dload` | `double` |
| Packet/byte | `spkts`, `dpkts`, `sttl`, `dttl`, `sloss`, `dloss`, `swin`, `dwin`, `smean`, `dmean` | `int` |
| Packet/byte lớn | `sbytes`, `dbytes`, `stcpb`, `dtcpb`, `response_body_len` | `bigint` |
| Timing | `sinpkt`, `dinpkt`, `sjit`, `djit`, `tcprtt`, `synack`, `ackdat` | `double` |
| Nội dung/kết nối | `trans_depth`, `ct_srv_src`, `ct_state_ttl`, `ct_dst_ltm`, `ct_src_dport_ltm`, `ct_dst_sport_ltm`, `ct_dst_src_ltm`, `is_ftp_login`, `ct_ftp_cmd`, `ct_flw_http_mthd`, `ct_src_ltm`, `ct_srv_dst`, `is_sm_ips_ports` | `int` |
| Nhãn đa lớp | `attack_cat` | `string` |
| Nhãn nhị phân | `label` | `int` (`0=Normal`, `1=Attack`) |

## Quy tắc dùng cột

- `label` là target bắt buộc của bài toán nhị phân.
- `attack_cat` chỉ dùng để thống kê/error analysis hoặc bài toán đa lớp mở rộng;
  tuyệt đối không đưa vào feature của mô hình nhị phân.
- `id` là định danh dòng và mặc định bị loại khỏi feature.
- `proto`, `service`, `state` được xử lý như categorical feature trong pipeline.
- Các cột còn lại là numeric feature; quy tắc làm sạch cụ thể được chốt ở Phase 3.

