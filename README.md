# grpc-client

Client Python per il servizio `TheaService` con:

- chiamata `getTags` iniziale;
- sottoscrizione `subscribeTags` su tutti i tag ricevuti;
- autenticazione **mTLS** con `client.crt`, `client.key`, `rootca.crt`;
- persistenza completa su DB (**PostgreSQL** o **MariaDB** selezionabile da parametro);
- log su file con nome contenente il timestamp di avvio;
- dashboard grafica con ricerca per tag o timestamp, rate segnali e throughput.

## 0) Setup ambiente e installazione librerie


> Requisiti consigliati: Python **3.11+**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
```

Se vuoi verificare che gli import richiesti dal codice siano risolti:

```bash
python - <<'PY'
import grpc
import google.protobuf
import sqlalchemy
import pandas
import plotly
import streamlit
print('OK: tutte le librerie principali sono installate')
PY
```

## 1) Generazione stub gRPC

Il proto si trova in `proto/thea.proto`.

```bash
python -m grpc_tools.protoc \
  -I ./proto \
  --python_out=. \
  --grpc_python_out=. \
  ./proto/thea.proto
```

Questo comando genera `thea_pb2.py` e `thea_pb2_grpc.py` in root progetto.

## 2) Avvio client

```bash
python -m thea_client.client \
  --target <HOST:PORT> \
  --grpc-host <TLS_SERVER_HOSTNAME> \
  --rootca rootca.crt \
  --client-crt client.crt \
  --client-key client.key \
  --db-backend postgresql \
  --db-host 127.0.0.1 \
  --db-port 5432 \
  --db-name thea \
  --db-user user \
  --db-password pass \
  --rpc-service TheaQ.TheaService \
  --rpc-gettags getTags \
  --rpc-subscribetags subscribeTags
```

Per MariaDB:

```bash
--db-backend mariadb --db-port 3306
```

`--target` indica l'endpoint di connessione (`host:port`), mentre `--grpc-host` imposta l'hostname TLS usato per la validazione mTLS (CN/SAN del certificato server).

Se ricevi `StatusCode.UNIMPLEMENTED` con messaggio `Method not found`, configura i nomi RPC del server:

```bash
--rpc-service <Package.Service> --rpc-gettags <nome_metodo_get> --rpc-subscribetags <nome_metodo_subscribe>
```

Il client prova anche automaticamente alcuni service name comuni (es. `TheaService`, `TheaQ.TheaService`, `SqService`) per ridurre problemi di compatibilità.

### Keepalive / connessione persistente

Il client imposta keepalive HTTP/2 su gRPC e, in caso di errore, tenta automaticamente la riconnessione con backoff esponenziale.

### Logging richiesto

- Prima della subscribe viene scritto un log con **tutti i tag** ottenuti da `getTags`.
- Ogni segnale ricevuto viene loggato con `tag`, `value`, `timestamp`, `quality`.
- File log: `logs/thea_client_YYYYMMDD_HHMMSS.log`.

## 3) Avvio dashboard

```bash
streamlit run thea_client/dashboard.py -- \
  --db-backend postgresql \
  --db-host 127.0.0.1 \
  --db-port 5432 \
  --db-name thea \
  --db-user user \
  --db-password pass
```

> Esegui il comando dalla root del repository (`grpc-client`).

Funzionalità dashboard:
- filtro tag (contains);
- filtro intervallo timestamp (epoch ms);
- tabella segnali dal DB;
- metriche: totale segnali, rate (segnali/min), throughput (B/s);
- grafico temporale del rate per tag.

## 4) Dipendenze

Le librerie usate dal progetto sono in `requirements.txt`.

Comando unico:

```bash
python -m pip install -r requirements.txt
```

Pacchetti principali installati:
- `grpcio`, `grpcio-tools`, `protobuf` (client gRPC e generazione stub);
- `SQLAlchemy`, `psycopg2-binary`, `PyMySQL` (persistenza PostgreSQL/MariaDB);
- `streamlit`, `pandas`, `plotly` (dashboard e metriche).
