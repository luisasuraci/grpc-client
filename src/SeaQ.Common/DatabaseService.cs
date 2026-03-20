using System.Data;
using System.Data.Common;
using System.Text;
using Dapper;
using MySqlConnector;
using Npgsql;

namespace SeaQ.Common;

public sealed class DatabaseService
{
    private const int DefaultWindowSeconds = 900;
    private readonly DbConfig _config;

    public DatabaseService(DbConfig config)
    {
        _config = config;
    }

    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        var sql = _config.Backend.Equals("postgresql", StringComparison.OrdinalIgnoreCase)
            ? PostgreSqlSchemaSql
            : MariaDbSchemaSql;
        await connection.ExecuteAsync(new CommandDefinition(sql, cancellationToken: cancellationToken));
    }

    public async Task SaveSubscriptionTagsAsync(string runId, IEnumerable<string> tags, CancellationToken cancellationToken = default)
    {
        var capturedAt = DateTimeOffset.UtcNow;
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        const string sql = @"
INSERT INTO subscription_tags (run_id, tag, captured_at)
VALUES (@RunId, @Tag, @CapturedAt);";
        foreach (var tag in tags)
        {
            await connection.ExecuteAsync(new CommandDefinition(sql, new { RunId = runId, Tag = tag, CapturedAt = capturedAt }, transaction, cancellationToken: cancellationToken));
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task SaveSignalsAsync(IReadOnlyCollection<SignalInsertRow> signals, IReadOnlyCollection<SignalCastKeyInsertRow> castRows, CancellationToken cancellationToken = default)
    {
        if (signals.Count == 0)
        {
            return;
        }

        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        const string insertSignalsSql = @"
INSERT INTO signals (
    run_id, tag, quality, timestamp_ms, unit, value_type, value_text, payload_size_bytes, received_at
) VALUES (
    @RunId, @Tag, @Quality, @TimestampMs, @Unit, @ValueType, @ValueText, @PayloadSizeBytes, @ReceivedAt
);";

        foreach (var signal in signals)
        {
            await connection.ExecuteAsync(new CommandDefinition(insertSignalsSql, signal, transaction, cancellationToken: cancellationToken));
        }

        var upsertSql = _config.Backend.Equals("postgresql", StringComparison.OrdinalIgnoreCase)
            ? PostgreSqlUpsertCastKeySql
            : MariaDbUpsertCastKeySql;

        foreach (var castRow in castRows)
        {
            await connection.ExecuteAsync(new CommandDefinition(upsertSql, castRow, transaction, cancellationToken: cancellationToken));
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<DashboardQuery> BuildDashboardQueryAsync(
        string tagFilter,
        string startInput,
        string endInput,
        int page,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        var tagConditions = new List<string>();
        var parameters = new DynamicParameters();

        if (!string.IsNullOrWhiteSpace(tagFilter))
        {
            tagConditions.Add("LOWER(tag) LIKE LOWER(@TagFilter)");
            parameters.Add("TagFilter", $"%{tagFilter.Trim()}%");
        }

        var maxTimestampScope = await WithRetryAsync(async connection =>
        {
            var sql = "SELECT MAX(timestamp_ms) FROM signals";
            if (tagConditions.Count > 0)
            {
                sql += " WHERE " + string.Join(" AND ", tagConditions);
            }

            return await connection.ExecuteScalarAsync<long?>(new CommandDefinition(sql, parameters, cancellationToken: cancellationToken));
        }, cancellationToken);

        var timestampsAreSeconds = maxTimestampScope is not null && maxTimestampScope.Value < 1_000_000_000_000;
        var unitFactor = timestampsAreSeconds ? 1 : 1000;
        long? startRaw = null;
        long? endRaw = null;
        if (!string.IsNullOrWhiteSpace(startInput))
        {
            startRaw = ParseUiTimestamp(startInput, timestampsAreSeconds);
            parameters.Add("StartRaw", startRaw);
        }

        if (!string.IsNullOrWhiteSpace(endInput))
        {
            endRaw = ParseUiTimestamp(endInput, timestampsAreSeconds);
            parameters.Add("EndRaw", endRaw);
        }

        var whereParts = new List<string>(tagConditions);
        var usingDefaultWindow = false;
        if (startRaw is not null)
        {
            whereParts.Add("timestamp_ms >= @StartRaw");
        }

        if (endRaw is not null)
        {
            whereParts.Add("timestamp_ms <= @EndRaw");
        }

        if (startRaw is null && endRaw is null && maxTimestampScope is not null)
        {
            usingDefaultWindow = true;
            var defaultStart = maxTimestampScope.Value - (DefaultWindowSeconds * unitFactor);
            parameters.Add("DefaultStart", defaultStart);
            parameters.Add("DefaultEnd", maxTimestampScope.Value);
            whereParts.Add("timestamp_ms >= @DefaultStart");
            whereParts.Add("timestamp_ms <= @DefaultEnd");
        }

        var whereSql = whereParts.Count > 0 ? " WHERE " + string.Join(" AND ", whereParts) : string.Empty;
        var chartBucketSize = timestampsAreSeconds ? 60 : 60000;

        return new DashboardQuery(
            tagFilter.Trim(),
            startInput.Trim(),
            endInput.Trim(),
            Math.Max(page, 1),
            pageSize,
            startRaw,
            endRaw,
            maxTimestampScope,
            timestampsAreSeconds,
            usingDefaultWindow,
            unitFactor,
            chartBucketSize,
            whereSql,
            parameters);
    }

    public async Task<DashboardMetrics> GetMetricsAsync(DashboardQuery query, CancellationToken cancellationToken = default)
    {
        var total = await WithRetryAsync(connection =>
            connection.ExecuteScalarAsync<long>(new CommandDefinition("SELECT COUNT(id) FROM signals", cancellationToken: cancellationToken)), cancellationToken);

        var filteredTotal = await WithRetryAsync(connection =>
            connection.ExecuteScalarAsync<long>(new CommandDefinition($"SELECT COUNT(id) FROM signals{query.WhereSql}", query.Parameters, cancellationToken: cancellationToken)), cancellationToken);

        var summarySql = $@"
SELECT
    COUNT(id) AS CountSignals,
    COALESCE(SUM(payload_size_bytes), 0) AS TotalBytes,
    MIN(timestamp_ms) AS MinTimestamp,
    MAX(timestamp_ms) AS MaxTimestamp
FROM signals{query.WhereSql};";

        var summary = await WithRetryAsync(connection =>
            connection.QuerySingleAsync<MetricsQueryRow>(new CommandDefinition(summarySql, query.Parameters, cancellationToken: cancellationToken)), cancellationToken);

        var totalPages = filteredTotal > 0
            ? (int)Math.Ceiling(filteredTotal / (double)query.PageSize)
            : 1;

        return new DashboardMetrics(
            total,
            filteredTotal,
            summary.CountSignals,
            summary.TotalBytes,
            summary.MinTimestamp,
            summary.MaxTimestamp,
            query.TimestampsAreSeconds,
            query.UsingDefaultWindow,
            Math.Min(query.Page, totalPages),
            query.PageSize,
            totalPages,
            query.TagFilter,
            query.StartInput,
            query.EndInput,
            query.MaxTimestampScope);
    }

    public async Task<IReadOnlyList<TableSignalRow>> GetTableRowsAsync(DashboardQuery query, int page, int pageSize, CancellationToken cancellationToken = default)
    {
        var offset = Math.Max(page - 1, 0) * pageSize;
        var parameters = CloneParameters(query.Parameters);
        parameters.Add("OffsetRows", offset);
        parameters.Add("PageSize", pageSize);

        var sql = $@"
SELECT tag, timestamp_ms AS TimestampMs, value_text AS Value, value_type AS ValueType, quality, payload_size_bytes AS PayloadBytes
FROM signals{query.WhereSql}
ORDER BY timestamp_ms ASC, id ASC
LIMIT @PageSize OFFSET @OffsetRows;";

        var rows = await WithRetryAsync(connection =>
            connection.QueryAsync<TableSignalRow>(new CommandDefinition(sql, parameters, cancellationToken: cancellationToken)), cancellationToken);
        return rows.ToList();
    }

    public async Task<IReadOnlyList<ChartSignalRow>> GetChartRowsAsync(DashboardQuery query, CancellationToken cancellationToken = default)
    {
        var sql = $@"
SELECT
    tag,
    FLOOR(timestamp_ms / @ChartBucketSize) * @ChartBucketSize AS TimestampRaw,
    COUNT(id) AS Count
FROM signals{query.WhereSql}
GROUP BY tag, FLOOR(timestamp_ms / @ChartBucketSize)
ORDER BY tag, TimestampRaw;";
        var parameters = CloneParameters(query.Parameters);
        parameters.Add("ChartBucketSize", query.ChartBucketSize);

        var rows = await WithRetryAsync(connection =>
            connection.QueryAsync<ChartSignalRow>(new CommandDefinition(sql, parameters, cancellationToken: cancellationToken)), cancellationToken);
        return rows.ToList();
    }

    private async Task<T> WithRetryAsync<T>(Func<DbConnection, Task<T>> action, CancellationToken cancellationToken)
    {
        Exception? lastException = null;
        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                await using var connection = CreateConnection();
                await connection.OpenAsync(cancellationToken);
                return await action(connection);
            }
            catch (Exception exception) when (exception is DbException or TimeoutException)
            {
                lastException = exception;
                if (attempt == 3)
                {
                    throw;
                }

                await Task.Delay(TimeSpan.FromSeconds(0.5 * attempt), cancellationToken);
            }
        }

        throw lastException ?? new InvalidOperationException("Unexpected database retry failure.");
    }

    private static DynamicParameters CloneParameters(object parameters)
    {
        var clone = new DynamicParameters();
        if (parameters is DynamicParameters dynamicParameters)
        {
            foreach (var parameterName in dynamicParameters.ParameterNames)
            {
                clone.Add(parameterName, dynamicParameters.Get<object?>(parameterName));
            }
        }

        return clone;
    }

    private long ParseUiTimestamp(string value, bool timestampsAreSeconds)
    {
        if (!long.TryParse(value, out var parsed))
        {
            throw new ArgumentException($"Timestamp non valido: {value}");
        }

        return timestampsAreSeconds ? parsed / 1000 : parsed;
    }

    private DbConnection CreateConnection()
    {
        if (_config.Backend.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            var builder = new NpgsqlConnectionStringBuilder
            {
                Host = _config.Host,
                Port = _config.Port,
                Database = _config.Database,
                Username = _config.Username,
                Password = _config.Password,
                Pooling = true,
            };
            return new NpgsqlConnection(builder.ConnectionString);
        }

        if (_config.Backend.Equals("mariadb", StringComparison.OrdinalIgnoreCase) ||
            _config.Backend.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            var builder = new MySqlConnectionStringBuilder
            {
                Server = _config.Host,
                Port = (uint)_config.Port,
                Database = _config.Database,
                UserID = _config.Username,
                Password = _config.Password,
                Pooling = true,
            };
            return new MySqlConnection(builder.ConnectionString);
        }

        throw new ArgumentException("backend deve essere 'postgresql' oppure 'mariadb'");
    }

    private sealed record MetricsQueryRow(long CountSignals, long TotalBytes, long? MinTimestamp, long? MaxTimestamp);

    private const string PostgreSqlSchemaSql = @"
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
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
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    tag VARCHAR(255) NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_subscription_tags_run_id ON subscription_tags(run_id);
CREATE INDEX IF NOT EXISTS ix_subscription_tags_tag ON subscription_tags(tag);
CREATE INDEX IF NOT EXISTS ix_subscription_tags_captured_at ON subscription_tags(captured_at);";

    private const string MariaDbSchemaSql = @"
CREATE TABLE IF NOT EXISTS signals (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
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
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    tag VARCHAR(255) NOT NULL,
    captured_at DATETIME(6) NOT NULL,
    INDEX ix_subscription_tags_run_id (run_id),
    INDEX ix_subscription_tags_tag (tag),
    INDEX ix_subscription_tags_captured_at (captured_at)
);";

    private const string PostgreSqlUpsertCastKeySql = @"
INSERT INTO signals_cast_key (
    run_id, tag, quality, timestamp_ms, unit, value_type, value_text, payload_size_bytes, received_at, created_at, updated_at
) VALUES (
    @RunId, @Tag, @Quality, @TimestampMs, @Unit, @ValueType, @ValueText, @PayloadSizeBytes, @ReceivedAt, @CreatedAt, @UpdatedAt
)
ON CONFLICT (tag, timestamp_ms, value_text) DO UPDATE SET
    run_id = EXCLUDED.run_id,
    quality = EXCLUDED.quality,
    unit = EXCLUDED.unit,
    value_type = EXCLUDED.value_type,
    payload_size_bytes = EXCLUDED.payload_size_bytes,
    received_at = EXCLUDED.received_at,
    updated_at = EXCLUDED.updated_at;";

    private const string MariaDbUpsertCastKeySql = @"
INSERT INTO signals_cast_key (
    run_id, tag, quality, timestamp_ms, unit, value_type, value_text, payload_size_bytes, received_at, created_at, updated_at
) VALUES (
    @RunId, @Tag, @Quality, @TimestampMs, @Unit, @ValueType, @ValueText, @PayloadSizeBytes, @ReceivedAt, @CreatedAt, @UpdatedAt
)
ON DUPLICATE KEY UPDATE
    run_id = VALUES(run_id),
    quality = VALUES(quality),
    unit = VALUES(unit),
    value_type = VALUES(value_type),
    payload_size_bytes = VALUES(payload_size_bytes),
    received_at = VALUES(received_at),
    updated_at = VALUES(updated_at);";
}
