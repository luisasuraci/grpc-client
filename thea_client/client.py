from __future__ import annotations

import argparse
import logging
import os
import time
import uuid
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

import grpc
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.orm import Session

from thea_client.db import (
    DbConfig,
    SignalCastKeyRecord,
    SignalCastRecord,
    SignalRecord,
    create_db_engine,
    init_schema,
    save_subscription_tags,
)

import seaq_pb2


@dataclass(frozen=True)
class GrpcConfig:
    target: str
    grpc_host: str
    root_ca: str
    client_cert: str
    client_key: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Client SeaQ: getTags + subscribeTags con MTLS")
    parser.add_argument("--target", required=True, help="Target gRPC host:port usato per la connessione")
    parser.add_argument("--grpc-host", required=True, help="Hostname TLS atteso nel certificato server (CN/SAN)")

    parser.add_argument("--rootca", default="rootca.crt")
    parser.add_argument("--client-crt", default="client.crt")
    parser.add_argument("--client-key", default="client.key")

    parser.add_argument("--db-backend", choices=["postgresql", "mariadb"], required=True)
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", type=int, required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)

    parser.add_argument("--rpc-service", default="SeaQ.SqService", help="Nome servizio gRPC completo (es. SeaQ.SqService)")
    parser.add_argument("--rpc-gettags", default="getTags", help="Nome metodo unary per recuperare i tag")
    parser.add_argument("--rpc-subscribetags", default="subscribeTags", help="Nome metodo stream per la subscribe")

    parser.add_argument("--log-dir", default="logs")
    return parser.parse_args()


def setup_logger(log_dir: str) -> tuple[logging.Logger, str]:
    os.makedirs(log_dir, exist_ok=True)
    startup_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"seaq_client_{startup_ts}.log")

    logger = logging.getLogger("seaq-client")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger, log_path


def build_secure_channel(cfg: GrpcConfig) -> grpc.Channel:
    with open(cfg.root_ca, "rb") as f:
        root = f.read()
    with open(cfg.client_key, "rb") as f:
        key = f.read()
    with open(cfg.client_cert, "rb") as f:
        cert = f.read()

    creds = grpc.ssl_channel_credentials(root_certificates=root, private_key=key, certificate_chain=cert)
    options = [
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.http2.min_time_between_pings_ms", 10_000),
        ("grpc.http2.min_ping_interval_without_data_ms", 10_000),
    ]
    options += [
        ("grpc.ssl_target_name_override", cfg.grpc_host),
        ("grpc.default_authority", cfg.grpc_host),
    ]
    return grpc.secure_channel(cfg.target, creds, options=options)


def _rpc_candidates(primary_service: str) -> list[str]:
    candidates = [primary_service]
    fallback = [
        "SqService",
        "SeaQ.SqService",
        "sqbj.dataserver.service.grpc.definitions.SqService",
    ]
    for c in fallback:
        if c not in candidates:
            candidates.append(c)
    return candidates


def _build_unary_call(channel: grpc.Channel, service: str, method: str) -> Callable[[seaq_pb2.Void], seaq_pb2.SqSubscriptions]:
    return channel.unary_unary(
        f"/{service}/{method}",
        request_serializer=seaq_pb2.Void.SerializeToString,
        response_deserializer=seaq_pb2.SqSubscriptions.FromString,
    )


def _build_stream_call(channel: grpc.Channel, service: str, method: str) -> Callable[[seaq_pb2.SqSubscriptions], object]:
    return channel.unary_stream(
        f"/{service}/{method}",
        request_serializer=seaq_pb2.SqSubscriptions.SerializeToString,
        response_deserializer=seaq_pb2.SqSignals.FromString,
    )


def decode_signal_value(signal: seaq_pb2.SqSignal) -> tuple[str, str]:
    field = signal.WhichOneof("value")
    if field is None:
        return "none", ""
    value = getattr(signal, field)
    return field, str(value)


def cast_numeric_value(value_type: str, value_text: str) -> str:
    if value_type not in {"float", "integer", "long"}:
        return value_text
    try:
        numeric = Decimal(value_text)
        if not numeric.is_finite():
            return value_text
        return str(numeric.quantize(Decimal("0.01"), rounding=ROUND_DOWN))
    except (InvalidOperation, ValueError):
        return value_text


def upsert_signals_cast_key(session: Session, backend: str, rows: list[dict]) -> None:
    if not rows:
        return

    backend = backend.lower()
    if backend == "postgresql":
        stmt = postgresql_insert(SignalCastKeyRecord).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["tag", "timestamp_ms", "value_text"],
            set_={
                "run_id": stmt.excluded.run_id,
                "quality": stmt.excluded.quality,
                "unit": stmt.excluded.unit,
                "value_type": stmt.excluded.value_type,
                "payload_size_bytes": stmt.excluded.payload_size_bytes,
                "received_at": stmt.excluded.received_at,
                "updated_at": stmt.excluded.updated_at,
            },
        )
    elif backend in {"mariadb", "mysql"}:
        stmt = mysql_insert(SignalCastKeyRecord).values(rows)
        stmt = stmt.on_duplicate_key_update(
            run_id=stmt.inserted.run_id,
            quality=stmt.inserted.quality,
            unit=stmt.inserted.unit,
            value_type=stmt.inserted.value_type,
            payload_size_bytes=stmt.inserted.payload_size_bytes,
            received_at=stmt.inserted.received_at,
            updated_at=stmt.inserted.updated_at,
        )
    else:
        raise ValueError(f"backend non supportato per upsert: {backend}")

    session.execute(stmt)


def run_client(args: argparse.Namespace) -> None:
    logger, log_path = setup_logger(args.log_dir)
    run_id = uuid.uuid4().hex

    db_cfg = DbConfig(
        backend=args.db_backend,
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        username=args.db_user,
        password=args.db_password,
    )
    engine = create_db_engine(db_cfg)
    init_schema(engine)

    grpc_cfg = GrpcConfig(args.target, args.grpc_host, args.rootca, args.client_crt, args.client_key)
    logger.info("Avvio client run_id=%s log_file=%s", run_id, log_path)

    backoff = 1
    while True:
        try:
            channel = build_secure_channel(grpc_cfg)

            tags_response = None
            selected_service = None
            last_unimplemented = None
            for service_name in _rpc_candidates(args.rpc_service):
                get_tags = _build_unary_call(channel, service_name, args.rpc_gettags)
                try:
                    tags_response = get_tags(seaq_pb2.Void(), timeout=20)
                    selected_service = service_name
                    logger.info("Service gRPC selezionato: %s", selected_service)
                    break
                except grpc.RpcError as exc:
                    if exc.code() == grpc.StatusCode.UNIMPLEMENTED:
                        last_unimplemented = exc
                        logger.warning("Metodo non trovato su service=%s (/%s/%s)", service_name, service_name, args.rpc_gettags)
                        continue
                    raise

            if tags_response is None:
                if last_unimplemented is not None:
                    raise last_unimplemented
                raise RuntimeError("Impossibile risolvere il metodo getTags su tutti i service candidati")

            tags = list(tags_response.tag)
            logger.info("Lista tag ricevuta da getTags (%d): %s", len(tags), ", ".join(tags))
            save_subscription_tags(engine, run_id, tags)

            req = seaq_pb2.SqSubscriptions(tag=tags)
            subscribe_tags = _build_stream_call(channel, selected_service, args.rpc_subscribetags)
            stream = subscribe_tags(req)

            with Session(engine) as session:
                for packet in stream:
                    now = datetime.now(timezone.utc)
                    rows = []
                    cast_rows = []
                    cast_key_rows = []
                    for s in packet.signals:
                        value_type, value_text = decode_signal_value(s)
                        cast_value_text = cast_numeric_value(value_type, value_text)
                        quality_name = seaq_pb2.SqQuality.Name(s.quality)
                        logger.info("signal tag=%s value=%s ts=%d quality=%s", s.tag, value_text, s.timestamp, quality_name)
                        common_kwargs = dict(
                            run_id=run_id,
                            tag=s.tag,
                            quality=quality_name,
                            timestamp_ms=int(s.timestamp),
                            unit=s.unit if s.HasField("unit") else None,
                            value_type=value_type,
                            payload_size_bytes=s.ByteSize(),
                            received_at=now,
                        )
                        rows.append(SignalRecord(value_text=value_text, **common_kwargs))
                        cast_rows.append(SignalCastRecord(value_text=cast_value_text, **common_kwargs))
                        cast_key_rows.append(
                            dict(
                                value_text=cast_value_text,
                                created_at=now,
                                updated_at=now,
                                **common_kwargs,
                            )
                        )
                    if rows:
                        session.add_all(rows)
                        session.add_all(cast_rows)
                        upsert_signals_cast_key(session, args.db_backend, cast_key_rows)
                        session.commit()

            backoff = 1

        except grpc.RpcError as exc:
            logger.error("Errore gRPC (%s). Riprovo tra %ss", exc.code(), backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception:
            logger.exception("Errore non gestito nel loop principale. Riprovo tra %ss", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    run_client(parse_args())
