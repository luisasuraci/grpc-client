from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import and_, create_engine, func, select
from sqlalchemy.orm import Session

from thea_client.db import SignalRecord, throughput_bytes_per_second, rate_per_minute


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


def main() -> None:
    args = parse_args()
    engine = create_engine(db_uri(args), pool_pre_ping=True)

    st.set_page_config(page_title="Thea Signals Dashboard", layout="wide")
    st.title("TheaQ - Statistiche segnali")

    with st.sidebar:
        st.header("Filtri")
        tag_filter = st.text_input("Cerca tag (contains)", "")
        start = st.text_input("Timestamp start (ms)", "")
        end = st.text_input("Timestamp end (ms)", "")

    start_ms = int(start) if start.strip() else None
    end_ms = int(end) if end.strip() else None

    with Session(engine) as session:
        conds = []
        if tag_filter.strip():
            conds.append(SignalRecord.tag.ilike(f"%{tag_filter.strip()}%"))
        if start_ms is not None:
            conds.append(SignalRecord.timestamp_ms >= start_ms)
        if end_ms is not None:
            conds.append(SignalRecord.timestamp_ms <= end_ms)

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
        query = query.order_by(SignalRecord.timestamp_ms.desc()).limit(5000)

        rows = session.execute(query).all()
        total = session.execute(select(func.count(SignalRecord.id))).scalar_one()
        filtered = len(rows)
        rpm = rate_per_minute(session, start_ms, end_ms)
        thr = throughput_bytes_per_second(session, start_ms, end_ms)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Totale segnali", total)
    c2.metric("Segnali visualizzati", filtered)
    c3.metric("Rate", f"{rpm:.2f} segnali/min")
    c4.metric("Throughput", f"{thr:.2f} B/s")

    df = pd.DataFrame(rows, columns=["tag", "timestamp_ms", "value", "value_type", "quality", "payload_bytes"])
    if df.empty:
        st.warning("Nessun segnale trovato con i filtri impostati.")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    st.dataframe(df, use_container_width=True)

    chart_df = (
        df.groupby([pd.Grouper(key="timestamp", freq="1Min"), "tag"]).size().reset_index(name="count")
    )
    fig = px.line(chart_df, x="timestamp", y="count", color="tag", title="Rate segnali per tag")
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
