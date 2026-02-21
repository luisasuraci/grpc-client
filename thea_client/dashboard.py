from __future__ import annotations

import argparse
import math
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import and_, create_engine, func, select
from sqlalchemy.orm import Session

if __package__ in {None, ""}:
    from db import SignalRecord
else:
    from .db import SignalRecord


DEFAULT_WINDOW_SECONDS = 1800

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dashboard segnali Thea")
    parser.add_argument("--db-backend", choices=["postgresql", "mariadb"], required=True)
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", type=int, required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    return parser.parse_args()


def db_uri(args: argparse.Namespace) -> str:
    scheme = "postgresql+psycopg2" if args.db_backend == "postgresql" else "mysql+pymysql"
    return f"{scheme}://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}"


def _clear_filters() -> None:
    st.session_state["tag_filter"] = ""
    st.session_state["start_ts"] = ""
    st.session_state["end_ts"] = ""


def _normalize_epoch_ms(value: int | None) -> int | None:
    if value is None:
        return None
    return value * 1000 if value < 1_000_000_000_000 else value


def main() -> None:
    args = parse_args()
    engine = create_engine(db_uri(args), pool_pre_ping=True)

    st.set_page_config(page_title="Thea Signals Dashboard", layout="wide")
    st.title("TheaQ - Statistiche segnali")

    st.session_state.setdefault("tag_filter", "")
    st.session_state.setdefault("start_ts", "")
    st.session_state.setdefault("end_ts", "")

    st.subheader("Filtri")
    f1, f2, f3, f4 = st.columns([3, 2, 2, 1.5])
    with f1:
        tag_filter = st.text_input("Cerca tag (contains)", key="tag_filter")
    with f2:
        start = st.text_input("Timestamp start (ms)", key="start_ts")
    with f3:
        end = st.text_input("Timestamp end (ms)", key="end_ts")
    with f4:
        st.write("")
        st.write("")
        st.button("Pulisci filtri", use_container_width=True, on_click=_clear_filters)

    tag_conds = []
    if tag_filter.strip():
        tag_conds.append(SignalRecord.tag.ilike(f"%{tag_filter.strip()}%"))

    with Session(engine) as session:
        max_ts_scope_query = select(func.max(SignalRecord.timestamp_ms))
        if tag_conds:
            max_ts_scope_query = max_ts_scope_query.where(and_(*tag_conds))
        max_ts_scope = session.execute(max_ts_scope_query).scalar_one()

    timestamps_are_seconds = max_ts_scope is not None and int(max_ts_scope) < 1_000_000_000_000
    unit_factor = 1 if timestamps_are_seconds else 1000

    def ui_ms_to_raw(value: int) -> int:
        return value // 1000 if timestamps_are_seconds else value

    start_raw = ui_ms_to_raw(int(start)) if start.strip() else None
    end_raw = ui_ms_to_raw(int(end)) if end.strip() else None

    time_conds = []
    using_default_window = False
    if start_raw is not None:
        time_conds.append(SignalRecord.timestamp_ms >= start_raw)
    if end_raw is not None:
        time_conds.append(SignalRecord.timestamp_ms <= end_raw)

    if start_raw is None and end_raw is None and max_ts_scope is not None:
        using_default_window = True
        default_start = int(max_ts_scope) - (DEFAULT_WINDOW_SECONDS * unit_factor)
        time_conds.append(SignalRecord.timestamp_ms >= default_start)
        time_conds.append(SignalRecord.timestamp_ms <= int(max_ts_scope))

    conds = [*tag_conds, *time_conds]

    # Chiamata 1: statistiche globali (metriche dashboard)
    with Session(engine) as session:
        total = session.execute(select(func.count(SignalRecord.id))).scalar_one()
        filtered_count_query = select(func.count(SignalRecord.id))
        if conds:
            filtered_count_query = filtered_count_query.where(and_(*conds))
        filtered_total = session.execute(filtered_count_query).scalar_one()

        metrics_query = select(
            func.count(SignalRecord.id),
            func.coalesce(func.sum(SignalRecord.payload_size_bytes), 0),
            func.min(SignalRecord.timestamp_ms),
            func.max(SignalRecord.timestamp_ms),
        )
        if conds:
            metrics_query = metrics_query.where(and_(*conds))
        count_signals, total_bytes, min_ts, max_ts = session.execute(metrics_query).one()

        if not count_signals or min_ts is None or max_ts is None or max_ts == min_ts:
            rpm = 0.0
            thr = 0.0
        else:
            span_seconds = (int(max_ts) - int(min_ts)) / unit_factor
            rpm = (float(count_signals) / (span_seconds / 60.0)) if span_seconds > 0 else 0.0
            thr = (float(total_bytes) / span_seconds) if span_seconds > 0 else 0.0

    pager1, pager2 = st.columns([1, 1])
    with pager1:
        page_size = st.selectbox("Righe per pagina", [50, 100, 250, 500, 1000], index=0)
    total_pages = max(1, math.ceil(filtered_total / page_size)) if filtered_total else 1
    with pager2:
        page = st.number_input("Pagina", min_value=1, max_value=total_pages, value=1, step=1)
    offset = (int(page) - 1) * page_size

    chart_bucket_size = 60 if timestamps_are_seconds else 60000

    def load_table_rows() -> list[tuple]:
        with Session(engine) as session:
            base_filtered_query = select(
                SignalRecord.tag,
                SignalRecord.timestamp_ms,
                SignalRecord.value_text,
                SignalRecord.value_type,
                SignalRecord.quality,
                SignalRecord.payload_size_bytes,
            )
            if conds:
                base_filtered_query = base_filtered_query.where(and_(*conds))

            table_query = (
                base_filtered_query
                .order_by(SignalRecord.timestamp_ms.asc())
                .offset(offset)
                .limit(page_size)
            )
            return session.execute(table_query).all()

    def load_chart_rows() -> list[tuple]:
        with Session(engine) as session:
            chart_bucket_expr = (func.floor(SignalRecord.timestamp_ms / chart_bucket_size) * chart_bucket_size).label("bucket_ts")
            chart_query = select(
                SignalRecord.tag,
                chart_bucket_expr,
                func.count(SignalRecord.id).label("count"),
            )
            if conds:
                chart_query = chart_query.where(and_(*conds))
            chart_query = chart_query.group_by(SignalRecord.tag, chart_bucket_expr)
            return session.execute(chart_query).all()

    with st.spinner("Caricamento tabella e grafico segnali in parallelo..."):
        with ThreadPoolExecutor(max_workers=2) as executor:
            rows_future = executor.submit(load_table_rows)
            chart_future = executor.submit(load_chart_rows)
            rows = rows_future.result()
            chart_rows = chart_future.result()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Totale segnali", total)
    c2.metric("Segnali filtrati", filtered_total)
    c3.metric("Rate", f"{rpm:.2f} segnali/min")
    c4.metric("Throughput", f"{thr:.2f} B/s")

    df = pd.DataFrame(rows, columns=["tag", "timestamp_ms", "value", "value_type", "quality", "payload_bytes"])
    if df.empty:
        st.warning("Nessun segnale trovato con i filtri impostati.")
    else:
        df["timestamp_ms"] = df["timestamp_ms"].apply(_normalize_epoch_ms)
        df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        st.dataframe(df, use_container_width=True)

    st.caption(f"Pagina {int(page)} di {total_pages}")
    if using_default_window:
        st.caption("Filtro temporale di default attivo: ultimi 30 minuti.")

    chart_df = pd.DataFrame(chart_rows, columns=["tag", "timestamp_raw", "count"])
    if chart_df.empty:
        st.info("Nessun dato disponibile per il grafico con i filtri correnti.")
        return

    with st.spinner("Rendering grafico segnali (tutti i tag nel range filtrato) in corso..."):
        chart_df["timestamp_raw"] = pd.to_numeric(chart_df["timestamp_raw"], errors="coerce")
        chart_df = chart_df.dropna(subset=["timestamp_raw"])
        if chart_df.empty:
            st.info("Nessun dato timestamp valido disponibile per il grafico con i filtri correnti.")
            return
        chart_df["timestamp_ms"] = chart_df["timestamp_raw"].astype("int64").apply(_normalize_epoch_ms)
        chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp_ms"], unit="ms", utc=True)
        chart_df = chart_df.sort_values(["tag", "timestamp"])

        chart_df["timestamp_plot"] = chart_df["timestamp"]
        overlapping_points = chart_df["timestamp"].nunique() <= 2
        if overlapping_points:
            chart_df["_tag_idx"] = chart_df["tag"].astype("category").cat.codes
            chart_df["timestamp_plot"] = chart_df["timestamp"] + pd.to_timedelta(chart_df["_tag_idx"] * 120, unit="ms")

        fig = px.line(
            chart_df,
            x="timestamp_plot",
            y="count",
            color="tag",
            title="Rate segnali per tag",
            render_mode="webgl",
        )
        if len(chart_df) > 5000:
            fig.update_traces(mode="lines")
        else:
            fig.update_traces(mode="lines+markers", marker={"size": 8})
        fig.update_layout(xaxis_title="timestamp", yaxis_title="count")
        st.plotly_chart(fig, use_container_width=True)

    st.caption(f"Bucket grafico attuale: {chart_bucket_size} {'secondi' if timestamps_are_seconds else 'ms'} (tutti i tag).")

    if overlapping_points:
        st.caption("Nota: per evitare sovrapposizione visiva tra tag nello stesso minuto, il grafico applica un leggero offset orizzontale ai punti.")


if __name__ == "__main__":
    main()
