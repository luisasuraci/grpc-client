from __future__ import annotations

import argparse
import math

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import and_, create_engine, func, select
from sqlalchemy.orm import Session

if __package__ in {None, ""}:
    from db import SignalRecord
else:
    from .db import SignalRecord


DEFAULT_WINDOW_SECONDS = 3600

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

    with Session(engine) as session:
        tag_conds = []
        if tag_filter.strip():
            tag_conds.append(SignalRecord.tag.ilike(f"%{tag_filter.strip()}%"))

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

        total = session.execute(select(func.count(SignalRecord.id))).scalar_one()
        filtered_count_query = select(func.count(SignalRecord.id))
        if conds:
            filtered_count_query = filtered_count_query.where(and_(*conds))
        filtered_total = session.execute(filtered_count_query).scalar_one()

        pager1, pager2 = st.columns([1, 1])
        with pager1:
            page_size = st.selectbox("Righe per pagina", [50, 100, 250, 500, 1000], index=0)
        total_pages = max(1, math.ceil(filtered_total / page_size)) if filtered_total else 1
        with pager2:
            page = st.number_input("Pagina", min_value=1, max_value=total_pages, value=1, step=1)
        offset = (int(page) - 1) * page_size

        table_query = select(
            SignalRecord.tag,
            SignalRecord.timestamp_ms,
            SignalRecord.value_text,
            SignalRecord.value_type,
            SignalRecord.quality,
            SignalRecord.payload_size_bytes,
        )
        if conds:
            table_query = table_query.where(and_(*conds))
        table_query = table_query.order_by(SignalRecord.timestamp_ms.desc()).offset(offset).limit(page_size)
        with st.spinner("Caricamento tabella segnali..."):
            rows = session.execute(table_query).all()

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

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Totale segnali", total)
    c2.metric("Segnali filtrati", filtered_total)
    c3.metric("Rate", f"{rpm:.2f} segnali/min")
    c4.metric("Throughput", f"{thr:.2f} B/s")

    st.caption(f"Pagina {int(page)} di {total_pages}")
    if using_default_window:
        st.caption("Filtro temporale di default attivo: ultima ora.")

    df = pd.DataFrame(rows, columns=["tag", "timestamp_ms", "value", "value_type", "quality", "payload_bytes"])
    if df.empty:
        st.warning("Nessun segnale trovato con i filtri impostati.")
    else:
        df["timestamp_ms"] = df["timestamp_ms"].apply(_normalize_epoch_ms)
        df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        st.dataframe(df, use_container_width=True)

    chart_df = pd.DataFrame(rows, columns=["tag", "timestamp_ms", "value", "value_type", "quality", "payload_bytes"])
    if chart_df.empty:
        st.info("Nessun dato disponibile per il grafico con i filtri correnti.")
        return

    with st.spinner("Caricamento grafico segnali..."):
        chart_df = chart_df[["tag", "timestamp_ms"]].copy()
        chart_df["timestamp_ms"] = chart_df["timestamp_ms"].apply(_normalize_epoch_ms)
        chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp_ms"], unit="ms", utc=True)
        chart_df = chart_df.groupby([pd.Grouper(key="timestamp", freq="1Min"), "tag"]).size().reset_index(name="count")
        chart_df = chart_df.sort_values(["tag", "timestamp"])

    fig = px.line(chart_df, x="timestamp", y="count", color="tag", title="Rate segnali per tag")
    fig.update_traces(mode="lines+markers")
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
