"""Offline presentation dashboard for the UNSW-NB15 Spark IDS project."""

from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# ``streamlit run dashboard/app.py`` adds ``dashboard/`` rather than the project
# root to sys.path.  Keep the documented command runnable from a fresh shell.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.data_access import load_dashboard_bundle, load_json


st.set_page_config(page_title="Spark IDS · UNSW-NB15", page_icon="🛡️", layout="wide")
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.7rem; padding-bottom: 2rem; max-width: 1450px;}
      div[data-testid="stMetric"] {background: rgba(37, 99, 235, .055); border: 1px solid rgba(37, 99, 235, .14); padding: .8rem; border-radius: .8rem;}
      .hero {padding: 1.25rem 1.4rem; border-radius: 1rem; background: linear-gradient(120deg, #0f172a, #164e63); color: #f8fafc; margin-bottom: 1rem;}
      .hero h1 {margin: 0; font-size: 2rem;} .hero p {margin: .4rem 0 0; color: #bae6fd;}
      .note {border-left: 4px solid #0284c7; padding: .65rem 1rem; background: rgba(14,165,233,.06); border-radius: .35rem;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def data_bundle():
    return load_dashboard_bundle()


def dataframe(records: list[dict], *, columns: list[str] | None = None) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    if columns:
        return frame[[column for column in columns if column in frame.columns]]
    return frame


def overview_page(bundle):
    eda = bundle["eda"]
    classes = dataframe(eda["class_distribution"])
    total = int(classes["flow_count"].sum())
    attack = classes.loc[classes["traffic_class"] == "Attack"].iloc[0]
    protocols = dataframe(eda["protocol_attack_rate"])
    services = dataframe(eda["service_attack_rate"])

    st.subheader("Tổng quan dữ liệu huấn luyện")
    cols = st.columns(4)
    cols[0].metric("Tổng network flow", f"{total:,}")
    cols[1].metric("Attack", f"{int(attack['flow_count']):,}", f"{attack['percentage']:.2f}%")
    cols[2].metric("Protocol phổ biến", str(protocols.iloc[0]["proto"]).upper(), f"{int(protocols.iloc[0]['flow_count']):,} flow")
    cols[3].metric("Service phổ biến", str(services.iloc[0]["service"]), f"{int(services.iloc[0]['flow_count']):,} flow")

    left, right = st.columns([0.8, 1.2])
    with left:
        st.markdown("#### Phân phối lớp")
        chart = alt.Chart(classes).mark_arc(innerRadius=55).encode(
            theta=alt.Theta("flow_count:Q"), color=alt.Color("traffic_class:N", scale=alt.Scale(range=["#ef4444", "#38bdf8"])),
            tooltip=["traffic_class", alt.Tooltip("flow_count:Q", format=","), alt.Tooltip("percentage:Q", format=".2f")],
        )
        st.altair_chart(chart, width="stretch")
    with right:
        st.markdown("#### Nhóm tấn công")
        attacks = dataframe(eda["attack_category_distribution"]).head(10)
        chart = alt.Chart(attacks).mark_bar(cornerRadiusEnd=4).encode(
            x=alt.X("flow_count:Q", title="Flow"), y=alt.Y("attack_cat:N", sort="-x", title=None),
            color=alt.Color("percentage:Q", scale=alt.Scale(scheme="teals"), legend=None),
            tooltip=["attack_cat", alt.Tooltip("flow_count:Q", format=","), alt.Tooltip("percentage:Q", format=".2f")],
        )
        st.altair_chart(chart, width="stretch")

    st.markdown("#### Attack rate theo protocol và service")
    first, second = st.columns(2)
    with first:
        st.dataframe(protocols.head(12), width="stretch", hide_index=True)
    with second:
        st.dataframe(services.head(12), width="stretch", hide_index=True)
    st.markdown('<div class="note">EDA chỉ dùng Silver train. Các tỷ lệ là mô tả dữ liệu, không phải quan hệ nhân quả hoặc feature importance.</div>', unsafe_allow_html=True)


def confusion_frame(metrics):
    return pd.DataFrame(
        [[metrics["true_negative"], metrics["false_positive"]], [metrics["false_negative"], metrics["true_positive"]]],
        index=["Actual Normal", "Actual Attack"], columns=["Predicted Normal", "Predicted Attack"],
    )


def model_page(bundle):
    final_report = bundle["final_test_metrics"]
    metrics = final_report["evaluation"]["threshold_metrics"]
    ranking = final_report["evaluation"]["ranking_metrics"]
    comparison = bundle["model_comparison"]

    st.subheader("Mô hình cuối · Official test")
    st.caption(f"{final_report['model']['name']} · threshold khóa từ validation {final_report['model']['decision_threshold']:.2f}")
    cols = st.columns(6)
    for column, (label, value) in zip(
        cols,
        (("Precision", metrics["precision"]), ("Recall", metrics["recall"]), ("F1", metrics["f1"]),
         ("FPR", metrics["false_positive_rate"]), ("ROC-AUC", ranking["roc_auc"]), ("PR-AUC", ranking["pr_auc"])),
        strict=True,
    ):
        column.metric(label, f"{value:.4f}")

    left, right = st.columns([0.75, 1.25])
    with left:
        st.markdown("#### Confusion matrix · Test")
        st.dataframe(confusion_frame(metrics), width="stretch")
        st.warning("FPR test 29,16% cao hơn validation 11,72%. Kết quả được giữ nguyên; không chỉnh threshold theo test.")
    with right:
        st.markdown("#### So sánh candidate · Validation")
        candidates = dataframe(comparison["candidates"])
        display = candidates[["name", "threshold", "precision", "recall", "f1", "false_positive_rate", "roc_auc", "pr_auc", "fit_seconds"]].copy()
        display.columns = ["Candidate", "Threshold", "Precision", "Recall", "F1", "FPR", "ROC-AUC", "PR-AUC", "Fit (s)"]
        st.dataframe(display.style.highlight_max(subset=["F1", "ROC-AUC", "PR-AUC"], color="#bbf7d0"), width="stretch", hide_index=True)

    phase6 = bundle["model_comparison_full"] if "model_comparison_full" in bundle else None
    if phase6:
        selected = next(item for item in phase6["candidates"] if item["name"] == phase6["selection"]["selected_candidate"])
        curve = dataframe(selected["threshold_curve"])
        st.markdown("#### Trade-off threshold · Validation")
        metric_curve = curve.melt(id_vars="threshold", value_vars=["precision", "recall", "f1", "false_positive_rate"], var_name="metric", value_name="value")
        chart = alt.Chart(metric_curve).mark_line(point=True).encode(
            x=alt.X("threshold:Q", scale=alt.Scale(domain=[0.1, 0.9])), y=alt.Y("value:Q", scale=alt.Scale(domain=[0, 1])),
            color=alt.Color("metric:N"), tooltip=["threshold", "metric", alt.Tooltip("value:Q", format=".4f")],
        )
        st.altair_chart(chart, width="stretch")
        roc, pr = st.columns(2)
        with roc:
            st.markdown("##### ROC operating points · Validation")
            roc_frame = curve[["false_positive_rate", "recall", "threshold"]].sort_values("false_positive_rate")
            st.line_chart(roc_frame, x="false_positive_rate", y="recall")
        with pr:
            st.markdown("##### PR operating points · Validation")
            pr_frame = curve[["recall", "precision", "threshold"]].sort_values("recall")
            st.line_chart(pr_frame, x="recall", y="precision")
        st.caption("Các đường dùng 17 operating point validation; ROC-AUC/PR-AUC phía trên do Spark evaluator tính trên toàn bộ score test.")


def alerts_page(bundle):
    alerts_payload = bundle["alerts"]
    alerts = dataframe(alerts_payload["alerts"])
    st.subheader("Cảnh báo mẫu từ batch prediction")
    st.caption(f"Chỉ hiển thị tối đa {alerts_payload['limits']['alerts']} dòng đã export bằng Spark limit; nguồn có {alerts_payload['source']['source_rows']:,} prediction.")

    col1, col2, col3 = st.columns([1, 1, 1])
    protocols = ["Tất cả", *sorted(alerts["proto"].dropna().unique().tolist())]
    services = ["Tất cả", *sorted(alerts["service"].dropna().unique().tolist())]
    protocol = col1.selectbox("Protocol", protocols)
    service = col2.selectbox("Service", services)
    minimum = col3.slider("Xác suất Attack tối thiểu", 0.0, 1.0, 0.90, 0.01)
    filtered = alerts[alerts["attack_probability"] >= minimum]
    if protocol != "Tất cả":
        filtered = filtered[filtered["proto"] == protocol]
    if service != "Tất cả":
        filtered = filtered[filtered["service"] == service]
    st.dataframe(
        filtered[["id", "proto", "service", "state", "attack_cat", "attack_probability", "actual_class", "predicted_class"]],
        width="stretch", hide_index=True,
    )

    st.markdown("#### Phân tích lỗi mẫu")
    fp_tab, fn_tab = st.tabs(["False Positive · cảnh báo nhầm", "False Negative · bỏ sót"])
    columns = ["id", "proto", "service", "state", "attack_cat", "attack_probability", "actual_class", "predicted_class"]
    with fp_tab:
        st.dataframe(dataframe(alerts_payload["false_positives"], columns=columns), width="stretch", hide_index=True)
    with fn_tab:
        st.dataframe(dataframe(alerts_payload["false_negatives"], columns=columns), width="stretch", hide_index=True)
    st.error("Đây là mô hình nghiên cứu trên UNSW-NB15, không phải IDS production và không thay thế hệ thống giám sát mạng thực tế.")


def spark_page(bundle):
    benchmark = bundle["benchmark"]
    formats = benchmark["benchmarks"]["csv_vs_parquet"]
    cache = benchmark["benchmarks"]["cache_reuse"]
    partitions = benchmark["benchmarks"]["partition_count"]
    evidence = benchmark["execution_evidence"]

    st.subheader("Spark execution và benchmark")
    cols = st.columns(4)
    cols[0].metric("Parquet speedup", f"{formats['parquet_speedup_over_csv_median']:.2f}x")
    cols[1].metric("Cache action speedup", f"{cache['cache_speedup_median']:.2f}x")
    cols[2].metric("Task hoàn tất", evidence["status_tracker"]["completed_tasks_across_unique_stages"], "0 failed")
    cols[3].metric("Exchange trong plan", evidence["shuffle_exchange_nodes"])

    left, right = st.columns(2)
    with left:
        format_frame = pd.DataFrame(
            [{"format": "CSV", "median_seconds": formats["csv"]["timing"]["median_seconds"]},
             {"format": "Parquet", "median_seconds": formats["parquet"]["timing"]["median_seconds"]}]
        )
        st.markdown("#### CSV và Parquet")
        st.bar_chart(format_frame, x="format", y="median_seconds")
    with right:
        partition_frame = pd.DataFrame(
            [{"partitions": item["actual_partitions"], "aggregate_median_seconds": item["aggregate_timing"]["median_seconds"]} for item in partitions["results"]]
        )
        st.markdown("#### Aggregate theo số partition")
        st.line_chart(partition_frame, x="partitions", y="aggregate_median_seconds")

    st.info(f"Cache reuse median nhanh hơn, nhưng ba lần dùng có cache tốn {cache['three_reuses_with_cache_including_materialization_seconds']:.3f}s khi tính materialize, so với {cache['three_reuses_without_cache_total_seconds']:.3f}s không cache. Ước tính hòa vốn: {cache['estimated_break_even_reuses_from_medians']} reuse.")
    st.markdown("#### Partition → Task → Core → Stage → Job")
    st.code(
        "Action collect()\n"
        "  └─ Job\n"
        "     ├─ Stage: đọc input partitions → 1 Task / partition\n"
        "     ├─ Exchange: shuffle theo (label, proto)\n"
        "     └─ Stage: aggregate/order → output nhỏ\n\n"
        "local[2] → 1 local executor JVM → tối đa 2 Task đồng thời",
        language="text",
    )
    st.caption(f"Môi trường: Spark {benchmark['environment']['spark_version']}, {benchmark['environment']['spark_master']}, {benchmark['environment']['logical_cpu_count']} logical CPU, {benchmark['environment']['physical_memory_bytes'] / (1024**3):.1f} GB RAM.")


try:
    bundle = data_bundle()
    bundle["model_comparison_full"] = load_json("outputs/metrics/phase6_model_comparison.json")
except (FileNotFoundError, ValueError, KeyError) as error:
    st.error(f"Không thể nạp dashboard artifacts: {error}")
    st.stop()

st.markdown('<div class="hero"><h1>🛡️ Spark Network Intrusion Detection</h1><p>UNSW-NB15 · PySpark ML · Random Forest · Offline artifacts</p></div>', unsafe_allow_html=True)
page = st.sidebar.radio("Điều hướng", ["Tổng quan", "Mô hình", "Cảnh báo", "Spark benchmark"])
st.sidebar.caption(f"Artifact: {bundle['manifest']['generated_at']}")
st.sidebar.success("Offline ready · Không đọc dataset lớn")

if page == "Tổng quan":
    overview_page(bundle)
elif page == "Mô hình":
    model_page(bundle)
elif page == "Cảnh báo":
    alerts_page(bundle)
else:
    spark_page(bundle)
