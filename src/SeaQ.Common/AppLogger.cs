namespace SeaQ.Common;

public sealed class AppLogger : IDisposable
{
    private readonly object _gate = new();
    private readonly StreamWriter _writer;

    public AppLogger(string logDirectory)
    {
        Directory.CreateDirectory(logDirectory);
        var timestamp = DateTimeOffset.Now.ToString("yyyyMMdd_HHmmss");
        LogPath = Path.Combine(logDirectory, $"seaq_client_{timestamp}.log");
        _writer = new StreamWriter(File.Open(LogPath, FileMode.Create, FileAccess.Write, FileShare.Read))
        {
            AutoFlush = true,
        };
    }

    public string LogPath { get; }

    public void Info(string message) => Write("INFO", message);
    public void Warning(string message) => Write("WARN", message);
    public void Error(string message) => Write("ERROR", message);

    public void Exception(Exception exception, string message)
    {
        Write("ERROR", $"{message}{Environment.NewLine}{exception}");
    }

    private void Write(string level, string message)
    {
        var line = $"{DateTimeOffset.Now:O} {level} {message}";
        lock (_gate)
        {
            Console.WriteLine(line);
            _writer.WriteLine(line);
        }
    }

    public void Dispose() => _writer.Dispose();
}
