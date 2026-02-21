from __future__ import annotations

import argparse
import math

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import and_, create_engine, func, select
from sqlalchemy.orm import Session

if __package__ in {None, ""}:
    from db import SignalRecord, throughput_bytes_per_second, rate_per_minute
else:
    from .db import SignalRecord, throughput_bytes_per_second, rate_per_minute


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

    chart_window_min = st.selectbox(
        "Finestra grafico di default (usata solo se start/end sono vuoti)",
        options=["Tutto", 30, 60, 180, 360, 720, 1440],
        index=0,
    )

    start_ms = _normalize_epoch_ms(int(start)) if start.strip() else None
    end_ms = _normalize_epoch_ms(int(end)) if end.strip() else None

    with Session(engine) as session:
        tag_conds = []
        if tag_filter.strip():
            tag_conds.append(SignalRecord.tag.ilike(f"%{tag_filter.strip()}%"))

        conds = list(tag_conds)
        if start_ms is not None:
            conds.append(SignalRecord.timestamp_ms >= start_ms)
        if end_ms is not None:
            conds.append(SignalRecord.timestamp_ms <= end_ms)

        total = session.execute(select(func.count(SignalRecord.id))).scalar_one()

        filtered_count_query = select(func.count(SignalRecord.id))
        if conds:
            filtered_count_query = filtered_count_query.where(and_(*conds))
        filtered_total = session.execute(filtered_count_query).scalar_one()

        pager1, pager2 = st.columns([1, 1])
        with pager1:
            page_size = st.selectbox("Righe per pagina", [100, 250, 500, 1000], index=1)
        total_pages = max(1, math.ceil(filtered_total / page_size)) if filtered_total else 1
        with pager2:
            page = st.number_input("Pagina", min_value=1, max_value=total_pages, value=1, step=1)

        offset = (int(page) - 1) * page_size

        query = select(
            SignalRecord.tag,
            SignalRecord.timestamp_ms,
            SignalRecord.value_text,
            SignalRecord.value_type,
            SignalRecord.quality,
            SignalRecord.payload_size_bytes,
        )
        if conds:
            query = query.where(and_(*conds))
        query = query.order_by(SignalRecord.timestamp_ms.desc()).offset(offset).limit(page_size)

        rows = session.execute(query).all()

        rpm = rate_per_minute(session, start_ms, end_ms)
        thr = throughput_bytes_per_second(session, start_ms, end_ms)

        chart_conds = list(tag_conds)
        if start_ms is None and end_ms is None and chart_window_min != "Tutto":
            max_ts_query = select(func.max(SignalRecord.timestamp_ms))
            if chart_conds:
                max_ts_query = max_ts_query.where(and_(*chart_conds))
            max_ts = _normalize_epoch_ms(session.execute(max_ts_query).scalar_one())
            if max_ts is not None:
                chart_start_ms = int(max_ts) - int(chart_window_min) * 60 * 1000
                chart_conds.append(SignalRecord.timestamp_ms >= chart_start_ms)
        else:
            if start_ms is not None:
                chart_conds.append(SignalRecord.timestamp_ms >= start_ms)
            if end_ms is not None:
                chart_conds.append(SignalRecord.timestamp_ms <= end_ms)

        chart_query = select(SignalRecord.tag, SignalRecord.timestamp_ms)
        if chart_conds:
            chart_query = chart_query.where(and_(*chart_conds))
        chart_rows = session.execute(chart_query).all()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Totale segnali", total)
    c2.metric("Segnali filtrati", filtered_total)
    c3.metric("Rate", f"{rpm:.2f} segnali/min")
    c4.metric("Throughput", f"{thr:.2f} B/s")

    st.caption(f"Pagina {int(page)} di {total_pages}")

    df = pd.DataFrame(rows, columns=["tag", "timestamp_ms", "value", "value_type", "quality", "payload_bytes"])
    if df.empty:
        st.warning("Nessun segnale trovato con i filtri impostati.")
    else:
        df["timestamp_ms"] = df["timestamp_ms"].apply(_normalize_epoch_ms)
        df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        st.dataframe(df, use_container_width=True)

    chart_df = pd.DataFrame(chart_rows, columns=["tag", "timestamp_ms"])
    if chart_df.empty:
        st.info("Nessun dato disponibile per il grafico con i filtri correnti.")
        return

    chart_df["timestamp_ms"] = chart_df["timestamp_ms"].apply(_normalize_epoch_ms)
    chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp_ms"], unit="ms", utc=True)
    chart_df = chart_df.groupby([pd.Grouper(key="timestamp", freq="1Min"), "tag"]).size().reset_index(name="count")
    chart_df = chart_df.sort_values(["tag", "timestamp"])

    fig = px.line(chart_df, x="timestamp", y="count", color="tag", title="Rate segnali per tag", markers=True)
    fig.update_traces(mode="lines+markers")
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
