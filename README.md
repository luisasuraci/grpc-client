# grpc-client

Client **C# / .NET 8** per il servizio `SqService`, con logica equivalente tra client gRPC, persistenza completa su DB e dashboard web.

## Funzionalità

Il progetto include due applicazioni C#:

- `SeaQ.GrpcClient`: client gRPC con mTLS che:
  - chiama `getTags` all'avvio;
  - salva tutti i tag ricevuti nella tabella `subscription_tags`;
  - esegue la subscribe `subscribeTags` sui tag ricevuti;
  - persiste ogni segnale in `signals`;
  - persiste anche la vista deduplicata `signals_cast_key` con cast numerico e arrotondamento matematico a 2 decimali;
  - scrive log su console e file `logs/seaq_client_YYYYMMDD_HHMMSS.log`;
  - gestisce keepalive HTTP/2 e riconnessione automatica con backoff esponenziale.
- `SeaQ.Dashboard`: dashboard web ASP.NET Core che mostra:
  - filtro tag (`contains`);
  - filtro timestamp start/end in epoch ms;
  - tabella segnali paginata;
  - metriche: totale segnali, segnali filtrati, rate segnali/min, throughput B/s;
  - grafico rate per tag con bucket temporali;
  - finestra di default sugli ultimi 15 minuti se non sono impostati filtri temporali.

## Struttura repository

- `src/SeaQ.Common`: libreria condivisa con modelli, parser CLI, log, schema SQL e accesso DB.
- `src/SeaQ.GrpcClient`: eseguibile console per acquisizione gRPC e persistenza.
- `src/SeaQ.Dashboard`: dashboard web.
- `proto/seaq.proto`: contratto gRPC utilizzato per generare i tipi C#.

## Requisiti

- .NET SDK 8.0+
- PostgreSQL oppure MariaDB
- certificati `client.crt`, `client.key`, `rootca.crt`

## Restore e build

```bash
dotnet restore SeaQ.Client.sln
dotnet build SeaQ.Client.sln
```

## Avvio client gRPC

```bash
dotnet run --project src/SeaQ.GrpcClient -- \
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
  --rpc-service SeaQ.SqService \
  --rpc-gettags getTags \
  --rpc-subscribetags subscribeTags
```

Per MariaDB:

```bash
--db-backend mariadb --db-port 3306
```

Parametri principali:

- `--target`: endpoint gRPC `host:port`.
- `--grpc-host`: hostname TLS usato per validare il certificato server.
- `--rpc-service`, `--rpc-gettags`, `--rpc-subscribetags`: override dei nomi RPC.
- `--log-dir`: directory dei log, default `logs`.

Il client prova automaticamente anche alcuni service name alternativi:

- `SqService`
- `SeaQ.SqService`
- `sqbj.dataserver.service.grpc.definitions.SqService`

## Avvio dashboard

```bash
dotnet run --project src/SeaQ.Dashboard -- \
  --db-backend postgresql \
  --db-host 127.0.0.1 \
  --db-port 5432 \
  --db-name thea \
  --db-user user \
  --db-password pass
```

Per cambiare porta HTTP:

```bash
--urls http://0.0.0.0:8080
```

Poi apri il browser su `http://localhost:5000` oppure sull'URL configurato.

## Note DB

Le applicazioni creano automaticamente le tabelle:

- `signals`
- `signals_cast_key`
- `subscription_tags`

`signals_cast_key` usa chiave primaria composta `tag,timestamp_ms,value_text` e upsert backend-specifico:

- PostgreSQL: `ON CONFLICT ... DO UPDATE`
- MariaDB: `ON DUPLICATE KEY UPDATE`

## Proto

Se modifichi `proto/seaq.proto`, i tipi C# vengono rigenerati automaticamente in build tramite `Grpc.Tools`.
