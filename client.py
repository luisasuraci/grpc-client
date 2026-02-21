from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import grpc

import seaq_pb2
import seaq_pb2_grpc
from storage import PersistedSignal, SignalStore


def now_ts() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")


def configure_logger(log_dir: Path, startup_ts: str) -> tuple[logging.Logger, Path]:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"signals_{startup_ts}.log"

    logger = logging.getLogger("seaq-client")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger, log_file


def load_tls_credentials(client_crt: Path, client_key: Path, root_ca: Path) -> grpc.ChannelCredentials:
    cert_chain = client_crt.read_bytes()
    private_key = client_key.read_bytes()
    root_cert = root_ca.read_bytes()
    return grpc.ssl_channel_credentials(root_certificates=root_cert, private_key=private_key, certificate_chain=cert_chain)


def sqsignal_value_to_text(signal: seaq_pb2.SqSignal) -> str:
    field = signal.WhichOneof("value")
    if field is None:
        return "<no-value>"
    value = getattr(signal, field)
    return f"{field}={value}"


def run(args: argparse.Namespace) -> None:
    startup_ts = now_ts()
    logger, log_file = configure_logger(Path(args.log_dir), startup_ts)
    db_path = Path(args.data_dir) / f"stats_{startup_ts}.sqlite3"
    store = SignalStore(db_path=db_path)

    channel_options = [
        ("grpc.keepalive_time_ms", args.keepalive_time_ms),
        ("grpc.keepalive_timeout_ms", args.keepalive_timeout_ms),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.http2.min_time_between_pings_ms", 10000),
        ("grpc.http2.min_ping_interval_without_data_ms", 10000),
    ]

    creds = load_tls_credentials(Path(args.client_crt), Path(args.client_key), Path(args.rootca_crt))

    logger.info("Avvio client gRPC mTLS verso %s", args.target)
    logger.info("Log file: %s", log_file)
    logger.info("DB statistiche/segnali: %s", db_path)

    retry_seconds = 2

    while True:
        try:
            with grpc.secure_channel(args.target, creds, options=channel_options) as channel:
                grpc.channel_ready_future(channel).result(timeout=30)
                stub = seaq_pb2_grpc.SqServiceStub(channel)

                tags_response = stub.getTags(seaq_pb2.Void(), timeout=30)
                all_tags = list(tags_response.tag)

                logger.info("Ricevuti %d tag da getTags. Elenco completo:", len(all_tags))
                for tag in all_tags:
                    logger.info("GET_TAG %s", tag)

                subscription = seaq_pb2.SqSubscriptions(tag=all_tags)
                stream = stub.subscribeTags(subscription, wait_for_ready=True)
                logger.info("Sottoscrizione subscribeTags avviata con %d tag", len(all_tags))

                for packet in stream:
                    packet_bytes = packet.ByteSize()
                    for signal in packet.signals:
                        value_text = sqsignal_value_to_text(signal)
                        logger.info("SIGNAL tag=%s timestamp=%d quality=%s value=%s", signal.tag, signal.timestamp, seaq_pb2.SqQuality.Name(signal.quality), value_text)

                        store.insert_signal(
                            PersistedSignal(
                                tag=signal.tag,
                                timestamp=signal.timestamp,
                                quality=seaq_pb2.SqQuality.Name(signal.quality),
                                value=value_text,
                                unit=signal.unit if signal.HasField("unit") else None,
                                payload_bytes=packet_bytes,
                            )
                        )

                logger.warning("Stream subscribeTags terminato dal server, riconnessione...")

        except grpc.RpcError as exc:
            logger.exception("Errore RPC (%s). Riprovo tra %ss", exc.code(), retry_seconds)
            time.sleep(retry_seconds)
        except Exception:
            logger.exception("Errore inatteso. Riprovo tra %ss", retry_seconds)
            time.sleep(retry_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SeaQ gRPC client con mTLS, persistenza segnali e statistiche.")
    parser.add_argument("--target", required=True, help="Host:port del server gRPC")
    parser.add_argument("--client-crt", default="client.crt", help="Certificato client mTLS")
    parser.add_argument("--client-key", default="client.key", help="Chiave privata client mTLS")
    parser.add_argument("--rootca-crt", default="rootca.crt", help="Root CA certificato")
    parser.add_argument("--log-dir", default="logs", help="Directory file di log")
    parser.add_argument("--data-dir", default="data", help="Directory database sqlite")
    parser.add_argument("--keepalive-time-ms", type=int, default=20000)
    parser.add_argument("--keepalive-timeout-ms", type=int, default=10000)
    return parser


if __name__ == "__main__":
    run(build_parser().parse_args())
