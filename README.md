# grpc-client

Client Python per il servizio `SeaQ.SqService` che:

1. chiama `getTags`;
2. logga tutti i tag ricevuti;
3. usa l'elenco completo per invocare `subscribeTags`;
4. salva su file di log ogni segnale ricevuto (`tag`, `timestamp`, `quality`, `value`);
5. persiste segnali e statistiche su SQLite (no retention solo in memoria);
6. espone una dashboard Streamlit per ricerca per tag/timestamp e metriche rate/throughput.

## Requisiti

```bash
pip install grpcio grpcio-tools protobuf streamlit pandas
```

## Struttura

- `seaq.proto`: definizione proto del servizio.
- `client.py`: client mTLS con keepalive e riconnessione automatica.
- `storage.py`: persistenza segnali/statistiche su SQLite.
- `dashboard.py`: interfaccia grafica per ricerca e analisi.

## Generazione stubs gRPC

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. seaq.proto
```

Genera i file `seaq_pb2.py` e `seaq_pb2_grpc.py` necessari per l'esecuzione.

## Avvio client

```bash
python client.py \
  --target HOST:PORT \
  --client-crt client.crt \
  --client-key client.key \
  --rootca-crt rootca.crt
```

### Keepalive e connessione attiva

Il client imposta opzioni gRPC keepalive (`grpc.keepalive_*`) per mantenere attiva la connessione anche durante periodi di inattività del flusso.

### File prodotti

- Log runtime: `logs/signals_<startup_timestamp>.log`
- DB runtime: `data/stats_<startup_timestamp>.sqlite3`

Il nome del file log contiene il timestamp di avvio, come richiesto.

## Avvio dashboard

```bash
streamlit run dashboard.py
```

Funzionalità:
- ricerca per tag (contains);
- filtro per timestamp (epoch ms);
- tabella segnali;
- rate segnali/s e throughput bytes/s;
- trend temporale e top tag.
