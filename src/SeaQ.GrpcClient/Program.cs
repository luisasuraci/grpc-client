using System.Globalization;
using System.Net.Http;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using SeaQ;
using SeaQ.Common;

static int PrintUsageAndExit(string? error = null)
{
    if (!string.IsNullOrWhiteSpace(error))
    {
        Console.Error.WriteLine(error);
        Console.Error.WriteLine();
    }

    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run --project src/SeaQ.GrpcClient -- \\");
    Console.WriteLine("    --target <HOST:PORT> --grpc-host <TLS_SERVER_HOSTNAME> \\");
    Console.WriteLine("    --rootca rootca.crt --client-crt client.crt --client-key client.key \\");
    Console.WriteLine("    --db-backend postgresql|mariadb --db-host 127.0.0.1 --db-port 5432 \\");
    Console.WriteLine("    --db-name thea --db-user user --db-password pass \\");
    Console.WriteLine("    --rpc-service SeaQ.SqService --rpc-gettags getTags --rpc-subscribetags subscribeTags");
    return 1;
}

try
{
    var parsed = CliArguments.Parse(args);
    var grpcConfig = new GrpcConfig(
        parsed.GetRequired("target"),
        parsed.GetRequired("grpc-host"),
        parsed.GetOptional("rootca", "rootca.crt"),
        parsed.GetOptional("client-crt", "client.crt"),
        parsed.GetOptional("client-key", "client.key"));

    var dbConfig = new DbConfig(
        parsed.GetRequired("db-backend"),
        parsed.GetRequired("db-host"),
        parsed.GetRequiredInt("db-port"),
        parsed.GetRequired("db-name"),
        parsed.GetRequired("db-user"),
        parsed.GetRequired("db-password"));

    var rpcService = parsed.GetOptional("rpc-service", "SeaQ.SqService");
    var rpcGetTags = parsed.GetOptional("rpc-gettags", "getTags");
    var rpcSubscribeTags = parsed.GetOptional("rpc-subscribetags", "subscribeTags");
    var logDir = parsed.GetOptional("log-dir", "logs");

    using var logger = new AppLogger(logDir);
    var runId = Guid.NewGuid().ToString("N");
    var database = new DatabaseService(dbConfig);
    await database.InitializeSchemaAsync();
    logger.Info($"Avvio client run_id={runId} log_file={logger.LogPath}");

    var backoffSeconds = 1;
    while (true)
    {
        try
        {
            using var channel = BuildChannel(grpcConfig);
            var callInvoker = channel.CreateCallInvoker();

            SqSubscriptions? tagsResponse = null;
            string? selectedService = null;
            RpcException? lastUnimplemented = null;
            foreach (var serviceName in RpcCandidates(rpcService))
            {
                var getTagsMethod = new Method<Void, SqSubscriptions>(
                    MethodType.Unary,
                    serviceName,
                    rpcGetTags,
                    Marshallers.Create(request => request.ToByteArray(), Void.Parser.ParseFrom),
                    Marshallers.Create(response => response.ToByteArray(), SqSubscriptions.Parser.ParseFrom));

                try
                {
                    tagsResponse = await callInvoker.AsyncUnaryCall(
                        getTagsMethod,
                        null,
                        new CallOptions(deadline: DateTime.UtcNow.AddSeconds(20)),
                        new Void()).ResponseAsync;
                    selectedService = serviceName;
                    logger.Info($"Service gRPC selezionato: {selectedService}");
                    break;
                }
                catch (RpcException exception) when (exception.StatusCode == StatusCode.Unimplemented)
                {
                    lastUnimplemented = exception;
                    logger.Warning($"Metodo non trovato su service={serviceName} (/{serviceName}/{rpcGetTags})");
                }
            }

            if (tagsResponse is null || selectedService is null)
            {
                if (lastUnimplemented is not null)
                {
                    throw lastUnimplemented;
                }

                throw new InvalidOperationException("Impossibile risolvere il metodo getTags su tutti i service candidati.");
            }

            var tags = tagsResponse.Tag.ToList();
            logger.Info($"Lista tag ricevuta da getTags ({tags.Count}): {string.Join(", ", tags)}");
            await database.SaveSubscriptionTagsAsync(runId, tags);

            var subscribeMethod = new Method<SqSubscriptions, SqSignals>(
                MethodType.ServerStreaming,
                selectedService,
                rpcSubscribeTags,
                Marshallers.Create(request => request.ToByteArray(), SqSubscriptions.Parser.ParseFrom),
                Marshallers.Create(response => response.ToByteArray(), SqSignals.Parser.ParseFrom));

            using var call = callInvoker.AsyncServerStreamingCall(subscribeMethod, null, new CallOptions(), new SqSubscriptions { Tag = { tags } });
            await foreach (var packet in call.ResponseStream.ReadAllAsync())
            {
                var now = DateTimeOffset.UtcNow;
                var rows = new List<SignalInsertRow>();
                var castRows = new List<SignalCastKeyInsertRow>();
                foreach (var signal in packet.Signals)
                {
                    var (valueType, valueText) = DecodeSignalValue(signal);
                    var castValueText = SignalValueHelpers.CastNumericValue(valueType, valueText);
                    var quality = signal.Quality.ToString();
                    logger.Info($"signal tag={signal.Tag} value={valueText} ts={signal.Timestamp} quality={quality}");
                    var unit = signal.HasUnit ? signal.Unit : null;
                    rows.Add(new SignalInsertRow(
                        runId,
                        signal.Tag,
                        quality,
                        (long)signal.Timestamp,
                        unit,
                        valueType,
                        valueText,
                        signal.CalculateSize(),
                        now));
                    castRows.Add(new SignalCastKeyInsertRow(
                        runId,
                        signal.Tag,
                        quality,
                        (long)signal.Timestamp,
                        unit,
                        valueType,
                        castValueText,
                        signal.CalculateSize(),
                        now,
                        now,
                        now));
                }

                if (rows.Count > 0)
                {
                    await database.SaveSignalsAsync(rows, castRows);
                }
            }

            backoffSeconds = 1;
        }
        catch (RpcException exception)
        {
            logger.Error($"Errore gRPC ({exception.StatusCode}). Riprovo tra {backoffSeconds}s");
            await Task.Delay(TimeSpan.FromSeconds(backoffSeconds));
            backoffSeconds = Math.Min(backoffSeconds * 2, 30);
        }
        catch (Exception exception)
        {
            logger.Exception(exception, $"Errore non gestito nel loop principale. Riprovo tra {backoffSeconds}s");
            await Task.Delay(TimeSpan.FromSeconds(backoffSeconds));
            backoffSeconds = Math.Min(backoffSeconds * 2, 30);
        }
    }
}
catch (ArgumentException exception)
{
    Environment.ExitCode = PrintUsageAndExit(exception.Message);
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception);
    Environment.ExitCode = 1;
}

static GrpcChannel BuildChannel(GrpcConfig config)
{
    var clientCertificate = X509Certificate2.CreateFromPemFile(config.ClientCert, config.ClientKey);
    clientCertificate = new X509Certificate2(clientCertificate.Export(X509ContentType.Pfx));

    var trustedRoot = X509Certificate2.CreateFromPemFile(config.RootCa);
    var handler = new SocketsHttpHandler
    {
        KeepAlivePingDelay = TimeSpan.FromSeconds(30),
        KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
        EnableMultipleHttp2Connections = true,
        SslOptions =
        {
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            TargetHost = config.GrpcHost,
            ClientCertificates = new X509CertificateCollection { clientCertificate },
            RemoteCertificateValidationCallback = (_, certificate, _, sslPolicyErrors) =>
            {
                if (certificate is null)
                {
                    return false;
                }

                using var chain = new X509Chain();
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.Add(trustedRoot);
                chain.ChainPolicy.VerificationFlags = X509VerificationFlags.NoFlag;
                var hasNameMismatch = sslPolicyErrors.HasFlag(System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch);
                return !hasNameMismatch && chain.Build(new X509Certificate2(certificate));
            },
        },
    };

    var address = new Uri($"https://{config.Target}");
    return GrpcChannel.ForAddress(address, new GrpcChannelOptions
    {
        HttpHandler = handler,
    });
}

static IEnumerable<string> RpcCandidates(string primaryService)
{
    var candidates = new List<string> { primaryService };
    foreach (var service in new[]
             {
                 "SqService",
                 "SeaQ.SqService",
                 "sqbj.dataserver.service.grpc.definitions.SqService",
             })
    {
        if (!candidates.Contains(service, StringComparer.Ordinal))
        {
            candidates.Add(service);
        }
    }

    return candidates;
}

static (string ValueType, string ValueText) DecodeSignalValue(SqSignal signal)
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
