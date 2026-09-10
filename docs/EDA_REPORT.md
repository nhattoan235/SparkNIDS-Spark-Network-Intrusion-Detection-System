# EDA UNSW-NB15 bằng Spark SQL

Sinh lúc: `2026-09-10T20:00:29+07:00` từ Silver train `175341` dòng.

## Phân phối lớp

| traffic_class | flow_count | percentage |
|---|---|---|
| Attack | 119341 | 68.0622 |
| Normal | 56000 | 31.9378 |

Tỷ lệ lớp lớn/lớp nhỏ là **2.1311:1**. Attack chiếm đa số trong train; vì chi phí bỏ sót tấn công cao, các phase mô hình vẫn phải ưu tiên Recall/PR-AUC đồng thời theo dõi False Positive Rate thay vì dựa vào Accuracy.

![Phân phối lớp](../outputs/figures/class_distribution.svg)

## Nhóm tấn công

| attack_cat | flow_count | percentage |
|---|---|---|
| Normal | 56000 | 31.9378 |
| Generic | 40000 | 22.8127 |
| Exploits | 33393 | 19.0446 |
| Fuzzers | 18184 | 10.3706 |
| DoS | 12264 | 6.9944 |
| Reconnaissance | 10491 | 5.9832 |
| Analysis | 2000 | 1.1406 |
| Backdoor | 1746 | 0.9958 |
| Shellcode | 1133 | 0.6462 |
| Worms | 130 | 0.0741 |

![Nhóm tấn công](../outputs/figures/attack_category_distribution.svg)

## Protocol phổ biến và attack rate

| proto | flow_count | attack_count | attack_rate_percent |
|---|---|---|---|
| tcp | 79946 | 40825 | 51.0657 |
| udp | 63283 | 49361 | 78.0004 |
| unas | 12084 | 12084 | 100.0 |
| arp | 2859 | 0 | 0.0 |
| ospf | 2595 | 2531 | 97.5337 |
| sctp | 1150 | 1150 | 100.0 |
| any | 300 | 300 | 100.0 |
| gre | 225 | 225 | 100.0 |
| sun-nd | 201 | 201 | 100.0 |
| ipv6 | 201 | 201 | 100.0 |
| mobile | 201 | 201 | 100.0 |
| pim | 201 | 201 | 100.0 |
| swipe | 201 | 201 | 100.0 |
| rsvp | 200 | 200 | 100.0 |
| sep | 193 | 193 | 100.0 |

![Protocol phổ biến](../outputs/figures/protocol_flow_count.svg)

## Service phổ biến và attack rate

| service | flow_count | attack_count | attack_rate_percent |
|---|---|---|---|
| __unknown__ | 94168 | 57656 | 61.2267 |
| dns | 47294 | 39801 | 84.1566 |
| http | 18724 | 13376 | 71.4377 |
| smtp | 5058 | 3479 | 68.7821 |
| ftp-data | 3995 | 1443 | 36.1202 |
| ftp | 3428 | 2210 | 64.4691 |
| ssh | 1302 | 11 | 0.8449 |
| pop3 | 1105 | 1101 | 99.638 |
| dhcp | 94 | 94 | 100.0 |
| snmp | 80 | 79 | 98.75 |
| ssl | 56 | 56 | 100.0 |
| irc | 25 | 25 | 100.0 |
| radius | 12 | 10 | 83.3333 |

## Trạng thái kết nối và attack rate

| state | flow_count | attack_count | attack_rate_percent |
|---|---|---|---|
| int | 82275 | 76560 | 93.0538 |
| fin | 77825 | 40650 | 52.2326 |
| con | 13152 | 1053 | 8.0064 |
| req | 1991 | 1066 | 53.5409 |
| rst | 83 | 12 | 14.4578 |
| eco | 12 | 0 | 0.0 |
| no | 1 | 0 | 0.0 |
| par | 1 | 0 | 0.0 |
| urn | 1 | 0 | 0.0 |

## Hồ sơ đặc trưng số theo lớp

| traffic_class | avg_duration | median_duration | avg_source_bytes | avg_destination_bytes | avg_rate | avg_source_packets | avg_destination_packets |
|---|---|---|---|---|---|---|---|
| Attack | 1.519969 | 9e-06 | 11068.66 | 7364.46 | 133699.69 | 15.41 | 10.01 |
| Normal | 1.017177 | 0.038566 | 4105.7 | 31049.46 | 13799.31 | 30.73 | 38.06 |

## Tương quan số với label

| feature | correlation_with_label |
|---|---|
| sttl | 0.6927414405810736 |
| ct_state_ttl | 0.5777039809439706 |
| dload | -0.3937393736073811 |
| ct_dst_sport_ltm | 0.3572133373537483 |
| dmean | -0.3418063059609523 |
| rate | 0.3379785113733934 |
| swin | -0.33363339385650437 |
| dwin | -0.3196255209501331 |
| ct_src_dport_ltm | 0.3055787534730584 |
| ct_dst_src_ltm | 0.30385517566930986 |
| stcpb | -0.25500603332934657 |
| dtcpb | -0.2503395789366557 |

![Tương quan số](../outputs/figures/numeric_label_correlation.svg)

## Bằng chứng Spark SQL

Pipeline đã chạy **6** truy vấn Spark SQL có tên: `class_distribution`, `attack_category_distribution`, `protocol_attack_rate`, `service_attack_rate`, `state_attack_rate`, `numeric_profile_by_class`. Kết quả đầy đủ cho service, state và numeric profile nằm trong `outputs/metrics/phase3_eda.json`.

Không có thao tác `toPandas()` hoặc collect toàn bộ dữ liệu; chỉ các bảng aggregate đã giới hạn được đưa về driver.
