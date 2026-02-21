from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(page_title="SeaQ Signals Dashboard", layout="wide")
st.title("SeaQ Signals Dashboard")


def load_data(db_path: Path, tag_filter: str, from_ts: int | None, to_ts: int | None, limit: int) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    where = ["1=1"]
    params: list[object] = []

    if tag_filter:
        where.append("tag LIKE ?")
        params.append(f"%{tag_filter}%")

    if from_ts is not None:
        where.append("timestamp >= ?")
        params.append(from_ts)

    if to_ts is not None:
        where.append("timestamp <= ?")
        params.append(to_ts)

    sql = f"""
        SELECT tag, timestamp, quality, value, unit, payload_bytes, inserted_at
        FROM signals
        WHERE {' AND '.join(where)}
        ORDER BY timestamp DESC
        LIMIT ?
    """
    params.append(limit)

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def load_rate_throughput(db_path: Path, from_sec: int | None, to_sec: int | None) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    where = ["1=1"]
    params: list[object] = []

    if from_sec is not None:
        where.append("second_bucket >= ?")
        params.append(from_sec)
    if to_sec is not None:
        where.append("second_bucket <= ?")
        params.append(to_sec)

    sql = f"""
        SELECT second_bucket, signal_count, byte_count
        FROM per_second_stats
        WHERE {' AND '.join(where)}
        ORDER BY second_bucket ASC
    """
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()

    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["second_bucket"], unit="s", utc=True)
        df["rate_signals_s"] = df["signal_count"]
        df["throughput_bytes_s"] = df["byte_count"]
    return df


db_default = sorted(Path("data").glob("stats_*.sqlite3"))
selected_db = st.text_input("SQLite DB path", value=str(db_default[-1]) if db_default else "data/stats.sqlite3")

tag_filter = st.text_input("Filtro tag (contains)")
col1, col2 = st.columns(2)
with col1:
    from_ts = st.text_input("Timestamp minimo (epoch ms)")
with col2:
    to_ts = st.text_input("Timestamp massimo (epoch ms)")
limit = st.number_input("Massimo record", min_value=10, max_value=100000, value=1000, step=10)

from_ts_i = int(from_ts) if from_ts.strip() else None
to_ts_i = int(to_ts) if to_ts.strip() else None

path = Path(selected_db)
if not path.exists():
    st.error(f"DB non trovato: {path}")
    st.stop()

df = load_data(path, tag_filter, from_ts_i, to_ts_i, int(limit))
st.subheader("Segnali")
st.dataframe(df, use_container_width=True)

rate_df = load_rate_throughput(path, from_ts_i // 1000 if from_ts_i else None, to_ts_i // 1000 if to_ts_i else None)
st.subheader("Statistiche")

if rate_df.empty:
    st.info("Nessuna statistica disponibile per i filtri selezionati")
else:
    total_signals = int(rate_df["signal_count"].sum())
    total_bytes = int(rate_df["byte_count"].sum())
    duration_s = max(1, int(rate_df["second_bucket"].max() - rate_df["second_bucket"].min() + 1))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Totale segnali", total_signals)
    m2.metric("Totale bytes", total_bytes)
    m3.metric("Rate medio (segnali/s)", f"{total_signals / duration_s:.2f}")
    m4.metric("Throughput medio (bytes/s)", f"{total_bytes / duration_s:.2f}")

    st.line_chart(rate_df.set_index("timestamp")[["rate_signals_s", "throughput_bytes_s"]])

    per_tag = (
        df.groupby("tag", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    st.subheader("Top tag")
    st.dataframe(per_tag, use_container_width=True)
