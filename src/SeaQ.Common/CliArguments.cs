using System.Collections.ObjectModel;

namespace SeaQ.Common;

public sealed class CliArguments
{
    private readonly ReadOnlyDictionary<string, string> _values;

    private CliArguments(Dictionary<string, string> values)
    {
        _values = new ReadOnlyDictionary<string, string>(values);
    }

    public static CliArguments Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length; i++)
        {
            var current = args[i];
            if (!current.StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            if (i + 1 >= args.Length || args[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                values[current[2..]] = "true";
                continue;
            }

            values[current[2..]] = args[++i];
        }

        return new CliArguments(values);
    }

    public string GetRequired(string key)
    {
        if (_values.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        throw new ArgumentException($"Missing required argument --{key}");
    }

    public string GetOptional(string key, string defaultValue) =>
        _values.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : defaultValue;

    public int GetRequiredInt(string key)
    {
        var value = GetRequired(key);
        return int.TryParse(value, out var result)
            ? result
            : throw new ArgumentException($"Invalid integer value for --{key}: {value}");
    }

    public int GetOptionalInt(string key, int defaultValue)
    {
        if (!_values.TryGetValue(key, out var value) || string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        return int.TryParse(value, out var result)
            ? result
            : throw new ArgumentException($"Invalid integer value for --{key}: {value}");
    }

    public IReadOnlyDictionary<string, string> Values => _values;
}
