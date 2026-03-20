using System.Globalization;

namespace SeaQ.Common;

public sealed record DbConfig(
    string Backend,
    string Host,
    int Port,
    string Database,
    string Username,
    string Password);

public sealed record GrpcConfig(
    string Target,
    string GrpcHost,
    string RootCa,
    string ClientCert,
    string ClientKey);

public sealed record SignalInsertRow(
    string RunId,
    string Tag,
    string Quality,
    long TimestampMs,
    string? Unit,
    string ValueType,
    string ValueText,
    int PayloadSizeBytes,
    DateTimeOffset ReceivedAt);

public sealed record SignalCastKeyInsertRow(
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

public sealed record DashboardMetrics(
    long TotalSignals,
    long FilteredSignals,
    long CountSignals,
    long TotalBytes,
    long? MinTimestamp,
    long? MaxTimestamp,
    bool TimestampsAreSeconds,
    bool UsingDefaultWindow,
    int Page,
    int PageSize,
    int TotalPages,
    string TagFilter,
    string StartInput,
    string EndInput,
    long? MaxTimestampScope);

public sealed record TableSignalRow(
    string Tag,
    long TimestampMs,
    string Value,
    string ValueType,
    string Quality,
    int PayloadBytes);

public sealed record ChartSignalRow(string Tag, long TimestampRaw, long Count);

public sealed record DashboardQuery(
    string TagFilter,
    string StartInput,
    string EndInput,
    int Page,
    int PageSize,
    long? StartRaw,
    long? EndRaw,
    long? MaxTimestampScope,
    bool TimestampsAreSeconds,
    bool UsingDefaultWindow,
    int UnitFactor,
    int ChartBucketSize,
    string WhereSql,
    object Parameters);

public static class SignalValueHelpers
{
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

        return decimal.Round(numeric, 2, MidpointRounding.AwayFromZero).ToString("0.00", CultureInfo.InvariantCulture);
    }

    public static long? NormalizeEpochMs(long? value) => value is null
        ? null
        : value.Value < 1_000_000_000_000 ? value.Value * 1000 : value.Value;
}
