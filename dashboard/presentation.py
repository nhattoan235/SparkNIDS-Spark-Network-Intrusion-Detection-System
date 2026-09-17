"""Turn Spark demo artifacts into short, table-first teaching views."""

from __future__ import annotations

from typing import Any


STAGES: tuple[dict[str, Any], ...] = (
    {
        "number": "01",
        "title": "Chuẩn bị dữ liệu",
        "step_ids": ("data", "parquet", "lazy"),
    },
    {
        "number": "02",
        "title": "Spark xử lý phân tán",
        "step_ids": ("execution", "cache"),
    },
    {
        "number": "03",
        "title": "Phát hiện xâm nhập",
        "step_ids": ("mllib", "streaming"),
    },
)


STEP_COPY: dict[str, dict[str, Any]] = {
    "data": {
        "question": "Spark hiểu dữ liệu mạng như thế nào?",
        "spark_action": (
            'spark.read.parquet("silver")',
            'df.groupBy("label").count()',
        ),
        "reading_hint": "Số flow cho biết quy mô; bảng cấu trúc cột (schema) cho biết tên cột, kiểu dữ liệu và ý nghĩa.",
        "conclusion": "Spark đọc được tên cột, kiểu dữ liệu và số lượng flow.",
        "ids_link": "Cấu trúc cột đúng giúp các bước huấn luyện và dự đoán IDS dùng cùng dữ liệu.",
    },
    "parquet": {
        "question": "CSV hay Parquet giúp Spark đọc nhanh hơn?",
        "spark_action": (
            "spark.read.csv(...) / spark.read.parquet(...)",
            'filter("label = 1").groupBy("proto").count()',
            "collect()",
        ),
        "reading_hint": "Thời gian chạy là mức giữa của nhiều lần đo; số thấp hơn nghĩa là đọc nhanh hơn.",
        "conclusion": "Hai format cho cùng kết quả; thời gian đọc được đo riêng trên máy hiện tại.",
        "ids_link": "Parquet phù hợp để Spark đọc nhiều cột dữ liệu mạng lặp lại trong pipeline IDS.",
    },
    "lazy": {
        "question": "Khi nào Spark thực sự bắt đầu chạy?",
        "spark_action": (
            "filter() → groupBy() → orderBy()",
            "collect()  # action kích hoạt Job",
        ),
        "reading_hint": "Job bằng 0 trước collect() và tăng sau collect() nghĩa là Spark trì hoãn thực thi.",
        "conclusion": "Các phép biến đổi chỉ tạo kế hoạch; action mới kích hoạt việc tính toán.",
        "ids_link": "Spark có thể tối ưu cả chuỗi xử lý flow trước khi chạy thật.",
    },
    "execution": {
        "question": "Spark chia một công việc lớn ra sao?",
        "spark_action": (
            "filter(label) → groupBy(label, proto)",
            "đếm flow + cộng byte gửi/nhận → sắp xếp",
            "collect()  # lấy kết quả",
        ),
        "reading_hint": "Job là một đợt chạy, Stage là một chặng, Task là việc nhỏ trên một phần dữ liệu.",
        "conclusion": "Spark chia truy vấn thành chặng (Stage), rồi chia mỗi chặng thành việc nhỏ (Task) trên các phần dữ liệu.",
        "ids_link": "Dữ liệu mạng lớn được chia nhỏ để nhiều task cùng xử lý.",
    },
    "cache": {
        "question": "Lưu DataFrame vào bộ nhớ có luôn nhanh hơn không?",
        "spark_action": (
            "df.cache()",
            "df.count()  # nạp cache",
            "chạy lại cùng truy vấn",
        ),
        "reading_hint": "Thời gian chạy lấy từ nhiều lần đo; nạp lần đầu là chi phí riêng để xem xét.",
        "conclusion": "Cache có chi phí nạp ban đầu và chỉ có ích khi dữ liệu được dùng lại đủ nhiều.",
        "ids_link": "Có thể cache tập đặc trưng dùng nhiều lần khi thử hoặc so sánh mô hình IDS.",
    },
    "mllib": {
        "question": "Spark MLlib nhận ra một flow tấn công thế nào?",
        "spark_action": (
            "Feature Pipeline",
            "RandomForestClassifier.fit()",
            "model.transform()",
        ),
        "reading_hint": "Đây là mô hình mẫu: học trên dữ liệu học, đo trên dữ liệu kiểm tra; chưa phải kết quả kiểm thử cuối của đồ án.",
        "conclusion": "Mô hình đoán Normal/Attack; bảng cho thấy đoán đúng, báo nhầm và bỏ sót.",
        "ids_link": "Bước này minh họa huấn luyện mô hình mẫu; mô hình này chưa được lưu để dùng cho Streaming.",
    },
    "streaming": {
        "question": "Một flow mới được hệ thống xử lý thế nào?",
        "spark_action": (
            "readStream()",
            "model.transform(micro_batch)",
            "checkpoint ghi tiến độ",
        ),
        "reading_hint": "File Parquet mô phỏng flow mới; đây chưa phải dữ liệu bắt gói tin trực tiếp. Mỗi dòng kết quả là một đợt xử lý nhỏ (micro-batch).",
        "conclusion": "Mỗi đợt file mới được phân loại Normal/Attack; dấu mốc ghi nhớ đã đọc đến đâu.",
        "ids_link": "Bước này dùng mô hình đã lưu để phân loại flow mới; đây là nguyên mẫu cho luồng cảnh báo.",
    },
}


def stage_for_step(step_id: str) -> dict[str, Any]:
    for stage in STAGES:
        if step_id in stage["step_ids"]:
            return stage
    return {"number": "--", "title": "Demo Spark", "step_ids": (step_id,)}


def _result(step: dict[str, Any]) -> dict[str, Any]:
    return step.get("result", {}) or {}


def _evidence(step: dict[str, Any]) -> dict[str, Any]:
    result = _result(step)
    return step.get("evidence", {}) or result.get("evidence", {}) or {}


def _seconds(value: Any) -> str:
    try:
        return f"{float(value):.3f} giây"
    except (TypeError, ValueError):
        return "Chưa có"


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _reported(value: Any) -> Any:
    return "Chưa có số liệu" if value is None else value


def _data_tables(step: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result = _result(step)
    evidence = _evidence(step)
    distribution = result.get("label_distribution") or evidence.get("label_distribution") or []
    input_rows = [
        {
            "Nhãn": "Normal" if int(row.get("label", -1)) == 0 else "Attack",
            "Số flow": int(row.get("count", 0)),
        }
        for row in distribution
    ] or [{"Dữ liệu": "UNSW-NB15", "Định dạng": "Parquet"}]
    type_meanings = {
        "id": "Mã flow",
        "dur": "Thời lượng",
        "proto": "Giao thức",
        "service": "Dịch vụ",
        "state": "Trạng thái kết nối",
        "spkts": "Gói gửi",
        "dpkts": "Gói nhận",
        "sbytes": "Byte gửi",
        "label": "Normal / Attack",
    }
    schema = result.get("schema") or evidence.get("schema") or []
    output_rows = [
        {
            "Cột": row.get("name", "—"),
            "Kiểu": row.get("type", "—"),
            "Ý nghĩa": type_meanings.get(str(row.get("name")), "Đặc trưng mạng"),
        }
        for row in schema[:8]
    ] or [{"Cột": "—", "Kiểu": "—", "Ý nghĩa": "Chưa có schema"}]
    return input_rows, output_rows


def _parquet_tables(step: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result = _result(step)
    evidence = _evidence(step)
    benchmark = result.get("benchmark", {}) or {}
    input_rows = [
        {"Format": "CSV", "Schema lưu sẵn": "Không", "Lưu theo": "Dòng"},
        {"Format": "Parquet", "Schema lưu sẵn": "Có", "Lưu theo": "Cột"},
    ]
    output_rows = []
    for name, key in (("CSV", "csv"), ("Parquet", "parquet")):
        item = benchmark.get(key, {}) or {}
        timing = item.get("timing", {}) or {}
        size = item.get("size_bytes", evidence.get(f"{key}_size_bytes"))
        output_rows.append(
            {
                "Format": name,
                "Thời gian chạy": _seconds(timing.get("median_seconds")),
                "Dung lượng": f"{float(size) / 1_048_576:.1f} MB" if size else "Chưa có",
            }
        )
    return input_rows, output_rows


def _lazy_tables(step: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result = _result(step)
    before = result.get("job_ids_before_action", _evidence(step).get("job_ids_before_action", [])) or []
    after = result.get("job_ids_after_action", _evidence(step).get("job_ids_after_action", [])) or []
    input_rows = [
        {"Thứ tự": 1, "Lệnh": "filter()", "Spark đã chạy?": "Chưa"},
        {"Thứ tự": 2, "Lệnh": "groupBy()", "Spark đã chạy?": "Chưa"},
        {"Thứ tự": 3, "Lệnh": "collect()", "Spark đã chạy?": "Có"},
    ]
    output_rows = [
        {"Thời điểm": "Trước collect()", "Số Job": str(len(before)), "Thời gian action": "—"},
        {"Thời điểm": "Sau collect()", "Số Job": str(len(after)), "Thời gian action": "—"},
        {"Thời điểm": "Thời gian action", "Số Job": "—", "Thời gian action": _seconds(result.get("action_seconds"))},
    ]
    return input_rows, output_rows


def _execution_tables(step: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result = _result(step)
    tracker = result.get("status_tracker") or _evidence(step).get("status_tracker") or {}
    partitions = result.get("input_partitions")
    input_rows = [
        {"Dữ liệu": "Flow Normal / Attack", "Phần dữ liệu ban đầu": f"{partitions} phần" if partitions is not None else "Chưa có số liệu"},
        {"Dữ liệu": "Nhóm theo nhãn + giao thức", "Phần dữ liệu ban đầu": "Spark phân phối lại khi cần"},
    ]
    output_rows = [
        {"Ghi nhận": "Job", "Số lượng": _reported(tracker.get("job_count"))},
        {"Ghi nhận": "Stage", "Số lượng": _reported(tracker.get("unique_stage_count"))},
        {"Ghi nhận": "Task được tạo", "Số lượng": _reported(tracker.get("total_tasks_across_unique_stages"))},
        {"Ghi nhận": "Task hoàn tất ghi nhận", "Số lượng": _reported(tracker.get("completed_tasks_across_unique_stages"))},
        {"Ghi nhận": "Task lỗi", "Số lượng": _reported(tracker.get("failed_tasks_across_unique_stages"))},
        {"Ghi nhận": "Số Exchange trong kế hoạch", "Số lượng": _reported(result.get("shuffle_exchange_nodes"))},
    ]
    return input_rows, output_rows


def _execution_terms() -> list[dict[str, str]]:
    return [
        {
            "Spark gọi": "Job",
            "Hiểu đơn giản": "Một đợt Spark thực thi",
            "Trong ví dụ": "Chạy truy vấn để lấy bảng flow theo nhãn và giao thức",
        },
        {
            "Spark gọi": "Stage",
            "Hiểu đơn giản": "Một chặng của đợt chạy",
            "Trong ví dụ": "Đọc/lọc; chuyển dữ liệu rồi gom và sắp xếp",
        },
        {
            "Spark gọi": "Task",
            "Hiểu đơn giản": "Việc nhỏ trên một phần dữ liệu",
            "Trong ví dụ": "Xử lý các flow trong một phần dữ liệu (partition) của một Stage",
        },
    ]


def _cache_tables(step: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result = _result(step)
    without_cache = result.get("without_cache", {}) or {}
    with_cache = result.get("with_cache", {}) or {}
    input_rows = [
        {"Cách chạy": "Không cache", "Dữ liệu đọc từ": "Ổ đĩa / nguồn"},
        {"Cách chạy": "Có cache", "Dữ liệu đọc từ": "Bộ nhớ sau lần nạp"},
    ]
    output_rows = [
        {"Phép đo": "Không cache", "Thời gian chạy": _seconds(without_cache.get("median_seconds"))},
        {"Phép đo": "Nạp cache lần đầu", "Thời gian chạy": _seconds(result.get("cache_materialization_seconds"))},
        {"Phép đo": "Dùng lại cache", "Thời gian chạy": _seconds(with_cache.get("median_seconds"))},
    ]
    return input_rows, output_rows


def _mllib_tables(step: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result = _result(step)
    matrix = result.get("confusion_matrix", {}) or {}
    input_rows = [
        {"Tập dữ liệu": "Dữ liệu học (Train)", "Số flow": result.get("train_rows", "Chưa có số liệu"), "Dùng để": "Học mô hình"},
        {"Tập dữ liệu": "Dữ liệu kiểm tra (Validation)", "Số flow": result.get("validation_rows", "Chưa có số liệu"), "Dùng để": "Kiểm tra mô hình"},
    ]
    output_rows = [
        {"Kết quả": "Attack đoán đúng", "Số flow": _reported(matrix.get("true_positive"))},
        {"Kết quả": "Normal đoán đúng", "Số flow": _reported(matrix.get("true_negative"))},
        {"Kết quả": "Báo động nhầm", "Số flow": _reported(matrix.get("false_positive"))},
        {"Kết quả": "Bỏ sót tấn công", "Số flow": _reported(matrix.get("false_negative"))},
    ]
    return input_rows, output_rows


def _streaming_tables(step: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    report = _result(step).get("streaming_report", {}) or {}
    source = report.get("input", {}) or {}
    input_rows = [
        {
            "Nguồn": "File Parquet mô phỏng flow mới",
            "Số file": source.get("published_files", "—"),
            "Mỗi lượt": f"{source.get('max_files_per_trigger', '—')} file",
        }
    ]
    batches = report.get("batch_summaries", []) or []
    output_rows = [
        {
            "Đợt xử lý (batch)": row.get("stream_batch_id", "—"),
            "Flow vào": row.get("input_rows", 0),
            "Normal": row.get("normal_rows", 0),
            "Attack": row.get("alert_rows", 0),
        }
        for row in batches
    ] or [{"Đợt xử lý (batch)": "Chưa có số liệu", "Flow vào": "—", "Normal": "—", "Attack": "—"}]
    return input_rows, output_rows


def _execution_hint(step: dict[str, Any]) -> str:
    result = _result(step)
    tracker = result.get("status_tracker") or _evidence(step).get("status_tracker") or {}
    keys = (
        "job_count",
        "unique_stage_count",
        "total_tasks_across_unique_stages",
        "completed_tasks_across_unique_stages",
        "failed_tasks_across_unique_stages",
    )
    if not all(tracker.get(key) is not None for key in keys):
        return "Job là một đợt chạy, Stage là một chặng, Task là việc nhỏ trên một phần dữ liệu. Chưa đủ số liệu để đếm lần chạy này."
    return (
        f"Lần chạy này: {tracker['job_count']} Job, {tracker['unique_stage_count']} Stage, "
        f"{tracker['total_tasks_across_unique_stages']} Task được tạo; "
        f"{tracker['completed_tasks_across_unique_stages']} Task hoàn tất, "
        f"{tracker['failed_tasks_across_unique_stages']} Task lỗi. Task chưa hoàn tất không tự động là lỗi."
    )


def _cache_conclusion(step: dict[str, Any]) -> str:
    result = _result(step)
    without_cache = _number((result.get("without_cache", {}) or {}).get("median_seconds"))
    with_cache = _number((result.get("with_cache", {}) or {}).get("median_seconds"))
    materialize = _number(result.get("cache_materialization_seconds"))
    if without_cache is None or with_cache is None:
        return "Chưa đủ số liệu để kết luận cache nhanh hơn. Cache có chi phí nạp ban đầu và phù hợp khi dữ liệu được dùng lại nhiều lần."
    if with_cache < without_cache:
        comparison = f"dùng lại cache nhanh hơn ({_seconds(with_cache)} so với {_seconds(without_cache)})"
    elif with_cache > without_cache:
        comparison = f"dùng lại cache chậm hơn ({_seconds(with_cache)} so với {_seconds(without_cache)})"
    else:
        comparison = f"hai cách có thời gian gần như nhau ({_seconds(with_cache)})"
    materialize_note = f" Nạp cache lần đầu mất {_seconds(materialize)}." if materialize is not None else ""
    return f"Trong lần đo này, {comparison}.{materialize_note} Cache có lợi khi dữ liệu được dùng lại nhiều lần."


TABLE_BUILDERS = {
    "data": _data_tables,
    "parquet": _parquet_tables,
    "lazy": _lazy_tables,
    "execution": _execution_tables,
    "cache": _cache_tables,
    "mllib": _mllib_tables,
    "streaming": _streaming_tables,
}


def build_step_view(step: dict[str, Any]) -> dict[str, Any]:
    """Build the small, stable view rendered by one presentation step."""
    step_id = str(step.get("id", ""))
    copy = STEP_COPY.get(
        step_id,
        {
            "question": "Spark đang xử lý gì?",
            "spark_action": ("Spark DataFrame",),
            "reading_hint": "Đọc lần lượt từ đầu vào đến kết quả.",
            "conclusion": "Chưa có mô tả cho bước này.",
            "ids_link": "Bước này thuộc pipeline IDS.",
        },
    )
    builder = TABLE_BUILDERS.get(step_id)
    input_table, result_table = builder(step) if builder else ([{"Đầu vào": "—"}], [{"Kết quả": "—"}])
    return {
        "stage": stage_for_step(step_id),
        "question": copy["question"],
        "spark_action": list(copy["spark_action"]),
        "term_table": _execution_terms() if step_id == "execution" else [],
        "input_table": input_table,
        "result_table": result_table,
        "reading_hint": _execution_hint(step) if step_id == "execution" else copy["reading_hint"],
        "conclusion": _cache_conclusion(step) if step_id == "cache" else copy["conclusion"],
        "ids_link": copy["ids_link"],
    }
