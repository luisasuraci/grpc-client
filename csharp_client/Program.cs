using System.Data;
using System.Globalization;
using System.Text;
using Google.Protobuf;
using Grpc.Core;
using MySqlConnector;
using Npgsql;
using SeaQ;
using EmptyRequest = SeaQ.Void;

var options = ClientOptions.Parse(args);
var logger = FileLogger.Create(options.LogDir);
var runId = Guid.NewGuid().ToString("N");

logger.Info($"Avvio client C# run_id={runId} log_file={logger.LogPath}");
await using var database = DatabaseClientFactory.Create(options);
await database.InitializeSchemaAsync();

var backoffSeconds = 1;
while (true)
{
    Channel? channel = null;

    try
    {
        channel = GrpcChannelFactory.Build(options);
        var callInvoker = channel.CreateCallInvoker();

        SqSubscriptions? tagsResponse = null;
        string? selectedService = null;
        RpcException? lastUnimplemented = null;

        foreach (var serviceName in RpcHelpers.GetServiceCandidates(options.RpcService))
        {
            var getTagsMethod = RpcHelpers.BuildUnaryMethod<EmptyRequest, SqSubscriptions>(serviceName, options.RpcGetTags, EmptyRequest.Parser, SqSubscriptions.Parser);
            try
            {
                var call = callInvoker.AsyncUnaryCall(
                    getTagsMethod,
                    host: null,
                    new CallOptions(deadline: DateTime.UtcNow.AddSeconds(20)),
                    new EmptyRequest());

                tagsResponse = await call.ResponseAsync.ConfigureAwait(false);
                selectedService = serviceName;
                logger.Info($"Service gRPC selezionato: {selectedService}");
                break;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented)
            {
                lastUnimplemented = ex;
                logger.Warning($"Metodo non trovato su service={serviceName} (/{serviceName}/{options.RpcGetTags})");
            }
        }

        if (tagsResponse is null || selectedService is null)
        {
            throw lastUnimplemented ?? new InvalidOperationException("Impossibile risolvere il metodo getTags su tutti i service candidati");
        }

        var tags = tagsResponse.Tag.ToList();
        logger.Info($"Lista tag ricevuta da getTags ({tags.Count}): {string.Join(", ", tags)}");
        await database.SaveSubscriptionTagsAsync(runId, tags).ConfigureAwait(false);

        var subscribeMethod = RpcHelpers.BuildServerStreamingMethod<SqSubscriptions, SqSignals>(selectedService, options.RpcSubscribeTags, SqSubscriptions.Parser, SqSignals.Parser);
        using var call = callInvoker.AsyncServerStreamingCall(
            subscribeMethod,
            host: null,
            new CallOptions(),
            new SqSubscriptions { Tag = { tags } });

        while (await call.ResponseStream.MoveNext(CancellationToken.None).ConfigureAwait(false))
        {
            var packet = call.ResponseStream.Current;
            var now = DateTimeOffset.UtcNow;
            var rows = new List<SignalRow>();
            var castKeyRows = new List<SignalCastKeyRow>();

            foreach (var signal in packet.Signals)
            {
                var (valueType, valueText) = SignalValueDecoder.Decode(signal);
                var castValueText = SignalValueDecoder.CastNumericValue(valueType, valueText);
                var qualityName = signal.Quality.ToString();

                logger.Info($"signal tag={signal.Tag} value={valueText} ts={signal.Timestamp} quality={qualityName}");

                rows.Add(new SignalRow(
                    RunId: runId,
                    Tag: signal.Tag,
                    Quality: qualityName,
                    TimestampMs: checked((long)signal.Timestamp),
                    Unit: signal.HasUnit ? signal.Unit : null,
                    ValueType: valueType,
                    ValueText: valueText,
                    PayloadSizeBytes: signal.CalculateSize(),
                    ReceivedAt: now));

                castKeyRows.Add(new SignalCastKeyRow(
                    RunId: runId,
                    Tag: signal.Tag,
                    Quality: qualityName,
                    TimestampMs: checked((long)signal.Timestamp),
                    Unit: signal.HasUnit ? signal.Unit : null,
                    ValueType: valueType,
                    ValueText: castValueText,
                    PayloadSizeBytes: signal.CalculateSize(),
                    ReceivedAt: now,
                    CreatedAt: now,
                    UpdatedAt: now));
            }

            if (rows.Count > 0)
            {
                await database.SaveSignalsAsync(rows, castKeyRows).ConfigureAwait(false);
            }
        }

        backoffSeconds = 1;
    }
    catch (RpcException ex)
    {
        logger.Error($"Errore gRPC ({ex.StatusCode}). Riprovo tra {backoffSeconds}s");
        await Task.Delay(TimeSpan.FromSeconds(backoffSeconds)).ConfigureAwait(false);
        backoffSeconds = Math.Min(backoffSeconds * 2, 30);
    }
    catch (Exception ex)
    {
        logger.Exception($"Errore non gestito nel loop principale. Riprovo tra {backoffSeconds}s", ex);
        await Task.Delay(TimeSpan.FromSeconds(backoffSeconds)).ConfigureAwait(false);
        backoffSeconds = Math.Min(backoffSeconds * 2, 30);
    }
    finally
    {
        if (channel is not null)
        {
            await channel.ShutdownAsync().ConfigureAwait(false);
        }
    }
}

internal sealed record ClientOptions(
    string Target,
    string GrpcHost,
    string RootCa,
    string ClientCert,
    string ClientKey,
    string DbBackend,
    string DbHost,
    int DbPort,
    string DbName,
    string DbUser,
    string DbPassword,
    string RpcService,
    string RpcGetTags,
    string RpcSubscribeTags,
    string LogDir)
{
    public static ClientOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < args.Length; i++)
        {
            var current = args[i];
            if (!current.StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Argomento non valido: {current}");
            }

            if (i + 1 >= args.Length || args[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Valore mancante per {current}");
            }

            values[current[2..]] = args[++i];
        }

        string GetRequired(string name)
        {
            if (values.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }

            throw new ArgumentException($"Parametro richiesto mancante: --{name}");
        }

        var dbBackend = GetRequired("db-backend");
        if (!dbBackend.Equals("postgresql", StringComparison.OrdinalIgnoreCase) &&
            !dbBackend.Equals("mariadb", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("--db-backend deve essere 'postgresql' oppure 'mariadb'");
        }

        return new ClientOptions(
            Target: GetRequired("target"),
            GrpcHost: GetRequired("grpc-host"),
            RootCa: values.GetValueOrDefault("rootca", "rootca.crt"),
            ClientCert: values.GetValueOrDefault("client-crt", "client.crt"),
            ClientKey: values.GetValueOrDefault("client-key", "client.key"),
            DbBackend: dbBackend,
            DbHost: GetRequired("db-host"),
            DbPort: int.Parse(GetRequired("db-port"), CultureInfo.InvariantCulture),
            DbName: GetRequired("db-name"),
            DbUser: GetRequired("db-user"),
            DbPassword: GetRequired("db-password"),
            RpcService: values.GetValueOrDefault("rpc-service", "SeaQ.SqService"),
            RpcGetTags: values.GetValueOrDefault("rpc-gettags", "getTags"),
            RpcSubscribeTags: values.GetValueOrDefault("rpc-subscribetags", "subscribeTags"),
            LogDir: values.GetValueOrDefault("log-dir", "logs"));
    }
}

internal static class GrpcChannelFactory
{
    public static Channel Build(ClientOptions options)
    {
        var rootCertificates = File.ReadAllText(options.RootCa, Encoding.UTF8);
        var certificateChain = File.ReadAllText(options.ClientCert, Encoding.UTF8);
        var privateKey = File.ReadAllText(options.ClientKey, Encoding.UTF8);

        var credentials = new SslCredentials(rootCertificates, new KeyCertificatePair(certificateChain, privateKey));
        var channelOptions = new List<ChannelOption>
        {
            new("grpc.keepalive_time_ms", 30_000),
            new("grpc.keepalive_timeout_ms", 10_000),
            new("grpc.keepalive_permit_without_calls", 1),
            new("grpc.http2.max_pings_without_data", 0),
            new("grpc.http2.min_time_between_pings_ms", 10_000),
            new("grpc.http2.min_ping_interval_without_data_ms", 10_000),
            new("grpc.ssl_target_name_override", options.GrpcHost),
            new("grpc.default_authority", options.GrpcHost),
        };

        return new Channel(options.Target, credentials, channelOptions);
    }
}

internal static class RpcHelpers
{
    public static IEnumerable<string> GetServiceCandidates(string primaryService)
    {
        yield return primaryService;

        foreach (var fallback in new[]
                 {
                     "SqService",
                     "SeaQ.SqService",
                     "sqbj.dataserver.service.grpc.definitions.SqService",
                 })
        {
            if (!string.Equals(primaryService, fallback, StringComparison.Ordinal))
            {
                yield return fallback;
            }
        }
    }

    public static Method<TRequest, TResponse> BuildUnaryMethod<TRequest, TResponse>(
        string service,
        string method,
        MessageParser<TRequest> requestParser,
        MessageParser<TResponse> responseParser)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse>
    {
        return new Method<TRequest, TResponse>(
            MethodType.Unary,
            service,
            method,
            CreateMarshaller(requestParser),
            CreateMarshaller(responseParser));
    }

    public static Method<TRequest, TResponse> BuildServerStreamingMethod<TRequest, TResponse>(
        string service,
        string method,
        MessageParser<TRequest> requestParser,
        MessageParser<TResponse> responseParser)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse>
    {
        return new Method<TRequest, TResponse>(
            MethodType.ServerStreaming,
            service,
            method,
            CreateMarshaller(requestParser),
            CreateMarshaller(responseParser));
    }

    private static Marshaller<T> CreateMarshaller<T>(MessageParser<T> parser)
        where T : class, IMessage<T>
    {
        return Marshallers.Create(
            serializer: message => message.ToByteArray(),
            deserializer: data => parser.ParseFrom(data));
    }
}

internal static class SignalValueDecoder
{
    public static (string ValueType, string ValueText) Decode(SqSignal signal)
    {
        return signal.ValueCase switch
        {
            SqSignal.ValueOneofCase.Float => ("float", signal.Float.ToString(CultureInfo.InvariantCulture)),
            SqSignal.ValueOneofCase.Boolean => ("boolean", signal.Boolean.ToString()),
            SqSignal.ValueOneofCase.String => ("string", signal.String),
            SqSignal.ValueOneofCase.Integer => ("integer", signal.Integer.ToString(CultureInfo.InvariantCulture)),
            SqSignal.ValueOneofCase.Long => ("long", signal.Long.ToString(CultureInfo.InvariantCulture)),
            _ => ("none", string.Empty),
        };
    }

    public static string CastNumericValue(string valueType, string valueText)
    {
        if (valueType is not ("float" or "integer" or "long"))
        {
            return valueText;
        }

        if (!decimal.TryParse(valueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var numeric))
        {
            return valueText;
        }

        return Math.Round(numeric, 2, MidpointRounding.AwayFromZero).ToString("0.00", CultureInfo.InvariantCulture);
    }
}

internal sealed class FileLogger : IDisposable
{
    private readonly object _sync = new();
    private readonly StreamWriter _writer;

    private FileLogger(string logPath)
    {
        LogPath = logPath;
        _writer = new StreamWriter(File.Open(logPath, FileMode.Create, FileAccess.Write, FileShare.Read))
        {
            AutoFlush = true,
        };
    }

    public string LogPath { get; }

    public static FileLogger Create(string logDir)
    {
        Directory.CreateDirectory(logDir);
        var startupTs = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
        var logPath = Path.Combine(logDir, $"seaq_client_{startupTs}.log");
        return new FileLogger(logPath);
    }

    public void Info(string message) => Write("INFO", message);
    public void Warning(string message) => Write("WARN", message);
    public void Error(string message) => Write("ERROR", message);
    public void Exception(string message, Exception exception) => Write("ERROR", $"{message}: {exception}");

    private void Write(string level, string message)
    {
        var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss,fff} {level} {message}";
        lock (_sync)
        {
            Console.WriteLine(line);
            _writer.WriteLine(line);
        }
    }

    public void Dispose()
    {
        _writer.Dispose();
    }
}

internal readonly record struct SignalRow(
    string RunId,
    string Tag,
    string Quality,
    long TimestampMs,
    string? Unit,
    string ValueType,
    string ValueText,
    int PayloadSizeBytes,
    DateTimeOffset ReceivedAt);

internal readonly record struct SignalCastKeyRow(
    string RunId,
    string Tag,
    string Quality,
    long TimestampMs,
    string? Unit,
    string ValueType,
    string ValueText,
    int PayloadSizeBytes,
    DateTimeOffset ReceivedAt,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

internal abstract class DatabaseClient : IAsyncDisposable
{
    protected DatabaseClient(ClientOptions options)
    {
        Options = options;
    }

    protected ClientOptions Options { get; }

    public abstract Task InitializeSchemaAsync();
    public abstract Task SaveSubscriptionTagsAsync(string runId, IReadOnlyCollection<string> tags);
    public abstract Task SaveSignalsAsync(IReadOnlyCollection<SignalRow> rows, IReadOnlyCollection<SignalCastKeyRow> castKeyRows);
    public abstract ValueTask DisposeAsync();

    protected static void AddCommonSignalParameters(IDbCommand command, SignalRow row)
    {
        AddParameter(command, "run_id", row.RunId);
        AddParameter(command, "tag", row.Tag);
        AddParameter(command, "quality", row.Quality);
        AddParameter(command, "timestamp_ms", row.TimestampMs);
        AddParameter(command, "unit", row.Unit);
        AddParameter(command, "value_type", row.ValueType);
        AddParameter(command, "value_text", row.ValueText);
        AddParameter(command, "payload_size_bytes", row.PayloadSizeBytes);
        AddParameter(command, "received_at", row.ReceivedAt.UtcDateTime);
    }

    protected static void AddCommonCastKeyParameters(IDbCommand command, SignalCastKeyRow row)
    {
        AddParameter(command, "run_id", row.RunId);
        AddParameter(command, "tag", row.Tag);
        AddParameter(command, "quality", row.Quality);
        AddParameter(command, "timestamp_ms", row.TimestampMs);
        AddParameter(command, "unit", row.Unit);
        AddParameter(command, "value_type", row.ValueType);
        AddParameter(command, "value_text", row.ValueText);
        AddParameter(command, "payload_size_bytes", row.PayloadSizeBytes);
        AddParameter(command, "received_at", row.ReceivedAt.UtcDateTime);
        AddParameter(command, "created_at", row.CreatedAt.UtcDateTime);
        AddParameter(command, "updated_at", row.UpdatedAt.UtcDateTime);
    }

    protected static void AddParameter(IDbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}

internal static class DatabaseClientFactory
{
    public static DatabaseClient Create(ClientOptions options)
    {
        return options.DbBackend.Equals("postgresql", StringComparison.OrdinalIgnoreCase)
            ? new PostgreSqlDatabaseClient(options)
            : new MariaDbDatabaseClient(options);
    }
}

internal sealed class PostgreSqlDatabaseClient : DatabaseClient
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlDatabaseClient(ClientOptions options) : base(options)
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = options.DbHost,
            Port = options.DbPort,
            Database = options.DbName,
            Username = options.DbUser,
            Password = options.DbPassword,
            Pooling = true,
        };
        _dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
    }

    public override async Task InitializeSchemaAsync()
    {
        const string sql = """
            CREATE TABLE IF NOT EXISTS signals (
                id BIGSERIAL PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                tag VARCHAR(255) NOT NULL,
                quality VARCHAR(64) NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                unit VARCHAR(64) NULL,
                value_type VARCHAR(16) NOT NULL,
                value_text VARCHAR(255) NOT NULL,
                payload_size_bytes INTEGER NOT NULL,
                received_at TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_signals_run_id ON signals(run_id);
            CREATE INDEX IF NOT EXISTS ix_signals_tag ON signals(tag);
            CREATE INDEX IF NOT EXISTS ix_signals_timestamp_ms ON signals(timestamp_ms);
            CREATE INDEX IF NOT EXISTS ix_signals_received_at ON signals(received_at);

            CREATE TABLE IF NOT EXISTS signals_cast_key (
                tag VARCHAR(255) NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                value_text VARCHAR(255) NOT NULL,
                run_id VARCHAR(64) NOT NULL,
                quality VARCHAR(64) NOT NULL,
                unit VARCHAR(64) NULL,
                value_type VARCHAR(16) NOT NULL,
                payload_size_bytes INTEGER NOT NULL,
                received_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tag, timestamp_ms, value_text)
            );
            CREATE INDEX IF NOT EXISTS ix_signals_cast_key_run_id ON signals_cast_key(run_id);
            CREATE INDEX IF NOT EXISTS ix_signals_cast_key_received_at ON signals_cast_key(received_at);
            CREATE INDEX IF NOT EXISTS ix_signals_cast_key_created_at ON signals_cast_key(created_at);
            CREATE INDEX IF NOT EXISTS ix_signals_cast_key_updated_at ON signals_cast_key(updated_at);

            CREATE TABLE IF NOT EXISTS subscription_tags (
                id BIGSERIAL PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                tag VARCHAR(255) NOT NULL,
                captured_at TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_subscription_tags_run_id ON subscription_tags(run_id);
            CREATE INDEX IF NOT EXISTS ix_subscription_tags_tag ON subscription_tags(tag);
            CREATE INDEX IF NOT EXISTS ix_subscription_tags_captured_at ON subscription_tags(captured_at);
            """;

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public override async Task SaveSubscriptionTagsAsync(string runId, IReadOnlyCollection<string> tags)
    {
        if (tags.Count == 0)
        {
            return;
        }

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);

        foreach (var tag in tags)
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO subscription_tags (run_id, tag, captured_at) VALUES (@run_id, @tag, @captured_at)",
                connection,
                transaction);
            AddParameter(command, "run_id", runId);
            AddParameter(command, "tag", tag);
            AddParameter(command, "captured_at", DateTime.UtcNow);
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await transaction.CommitAsync().ConfigureAwait(false);
    }

    public override async Task SaveSignalsAsync(IReadOnlyCollection<SignalRow> rows, IReadOnlyCollection<SignalCastKeyRow> castKeyRows)
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);

        foreach (var row in rows)
        {
            await using var insertSignal = new NpgsqlCommand(
                """
                INSERT INTO signals (
                    run_id, tag, quality, timestamp_ms, unit, value_type, value_text, payload_size_bytes, received_at
                ) VALUES (
                    @run_id, @tag, @quality, @timestamp_ms, @unit, @value_type, @value_text, @payload_size_bytes, @received_at
                )
                """,
                connection,
                transaction);
            AddCommonSignalParameters(insertSignal, row);
            await insertSignal.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        foreach (var row in castKeyRows)
        {
            await using var upsertCastKey = new NpgsqlCommand(
                """
                INSERT INTO signals_cast_key (
                    run_id, tag, quality, timestamp_ms, unit, value_type, value_text, payload_size_bytes, received_at, created_at, updated_at
                ) VALUES (
                    @run_id, @tag, @quality, @timestamp_ms, @unit, @value_type, @value_text, @payload_size_bytes, @received_at, @created_at, @updated_at
                )
                ON CONFLICT (tag, timestamp_ms, value_text) DO UPDATE
                SET
                    run_id = EXCLUDED.run_id,
                    quality = EXCLUDED.quality,
                    unit = EXCLUDED.unit,
                    value_type = EXCLUDED.value_type,
                    payload_size_bytes = EXCLUDED.payload_size_bytes,
                    received_at = EXCLUDED.received_at,
                    updated_at = EXCLUDED.updated_at
                """,
                connection,
                transaction);
            AddCommonCastKeyParameters(upsertCastKey, row);
            await upsertCastKey.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await transaction.CommitAsync().ConfigureAwait(false);
    }

    public override ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}

internal sealed class MariaDbDatabaseClient : DatabaseClient
{
    private readonly string _connectionString;

    public MariaDbDatabaseClient(ClientOptions options) : base(options)
    {
        var builder = new MySqlConnectionStringBuilder
        {
            Server = options.DbHost,
            Port = (uint)options.DbPort,
            Database = options.DbName,
            UserID = options.DbUser,
            Password = options.DbPassword,
            Pooling = true,
        };
        _connectionString = builder.ConnectionString;
    }

    public override async Task InitializeSchemaAsync()
    {
        const string sql = """
            CREATE TABLE IF NOT EXISTS signals (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                tag VARCHAR(255) NOT NULL,
                quality VARCHAR(64) NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                unit VARCHAR(64) NULL,
                value_type VARCHAR(16) NOT NULL,
                value_text VARCHAR(255) NOT NULL,
                payload_size_bytes INT NOT NULL,
                received_at DATETIME(6) NOT NULL,
                INDEX ix_signals_run_id (run_id),
                INDEX ix_signals_tag (tag),
                INDEX ix_signals_timestamp_ms (timestamp_ms),
                INDEX ix_signals_received_at (received_at)
            );

            CREATE TABLE IF NOT EXISTS signals_cast_key (
                tag VARCHAR(255) NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                value_text VARCHAR(255) NOT NULL,
                run_id VARCHAR(64) NOT NULL,
                quality VARCHAR(64) NOT NULL,
                unit VARCHAR(64) NULL,
                value_type VARCHAR(16) NOT NULL,
                payload_size_bytes INT NOT NULL,
                received_at DATETIME(6) NOT NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                PRIMARY KEY (tag, timestamp_ms, value_text),
                INDEX ix_signals_cast_key_run_id (run_id),
                INDEX ix_signals_cast_key_received_at (received_at),
                INDEX ix_signals_cast_key_created_at (created_at),
                INDEX ix_signals_cast_key_updated_at (updated_at)
            );

            CREATE TABLE IF NOT EXISTS subscription_tags (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(64) NOT NULL,
                tag VARCHAR(255) NOT NULL,
                captured_at DATETIME(6) NOT NULL,
                INDEX ix_subscription_tags_run_id (run_id),
                INDEX ix_subscription_tags_tag (tag),
                INDEX ix_subscription_tags_captured_at (captured_at)
            );
            """;

        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new MySqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public override async Task SaveSubscriptionTagsAsync(string runId, IReadOnlyCollection<string> tags)
    {
        if (tags.Count == 0)
        {
            return;
        }

        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);

        foreach (var tag in tags)
        {
            await using var command = new MySqlCommand(
                "INSERT INTO subscription_tags (run_id, tag, captured_at) VALUES (@run_id, @tag, @captured_at)",
                connection,
                transaction);
            AddParameter(command, "run_id", runId);
            AddParameter(command, "tag", tag);
            AddParameter(command, "captured_at", DateTime.UtcNow);
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await transaction.CommitAsync().ConfigureAwait(false);
    }

    public override async Task SaveSignalsAsync(IReadOnlyCollection<SignalRow> rows, IReadOnlyCollection<SignalCastKeyRow> castKeyRows)
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);

        foreach (var row in rows)
        {
            await using var insertSignal = new MySqlCommand(
                """
                INSERT INTO signals (
                    run_id, tag, quality, timestamp_ms, unit, value_type, value_text, payload_size_bytes, received_at
                ) VALUES (
                    @run_id, @tag, @quality, @timestamp_ms, @unit, @value_type, @value_text, @payload_size_bytes, @received_at
                )
                """,
                connection,
                transaction);
            AddCommonSignalParameters(insertSignal, row);
            await insertSignal.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        foreach (var row in castKeyRows)
        {
            await using var upsertCastKey = new MySqlCommand(
                """
                INSERT INTO signals_cast_key (
                    run_id, tag, quality, timestamp_ms, unit, value_type, value_text, payload_size_bytes, received_at, created_at, updated_at
                ) VALUES (
                    @run_id, @tag, @quality, @timestamp_ms, @unit, @value_type, @value_text, @payload_size_bytes, @received_at, @created_at, @updated_at
                )
                ON DUPLICATE KEY UPDATE
                    run_id = VALUES(run_id),
                    quality = VALUES(quality),
                    unit = VALUES(unit),
                    value_type = VALUES(value_type),
                    payload_size_bytes = VALUES(payload_size_bytes),
                    received_at = VALUES(received_at),
                    updated_at = VALUES(updated_at)
                """,
                connection,
                transaction);
            AddCommonCastKeyParameters(upsertCastKey, row);
            await upsertCastKey.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await transaction.CommitAsync().ConfigureAwait(false);
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
