using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using HCI.Kafka.Avro.Contracts;
using HCI.Kafka.Avro.SchemaRegistry.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HCI.Kafka.Avro.Producer;

/// <summary>
/// Avro producer for MedicinalProduct events.
///
/// HOW AVRO SERIALISATION WORKS END-TO-END
/// ─────────────────────────────────────────────────────────────────────────────
/// 1. First message produced for a schema:
///    a. AvroSerializer calls SchemaRegistryClient.RegisterSchemaAsync(subject, schema)
///    b. Schema Registry returns a schema ID (e.g. 42)
///    c. Serializer writes: [0x00][schema_id_4bytes][avro_payload]
///    d. Schema ID is cached locally — no SR call for subsequent messages
///
/// 2. Subsequent messages (same schema):
///    a. Schema ID lookup hits local cache — zero network calls to SR
///    b. Serializer writes [0x00][cached_id][avro_payload]
///    c. Throughput is identical to JSON (no SR overhead)
///
///
/// The Avro serializer:
///   ✓ Validates the object against the registered schema at produce time
///   ✓ Embeds the schema ID in every message (consumers auto-discover schema)
///   ✓ Uses binary encoding (~40-60% smaller than equivalent JSON)
///   ✓ Enforces schema evolution rules — incompatible changes are rejected
///
/// SubjectNameStrategy:
///   Default: TopicNameStrategy → subject = "{topic}-value"
///   Alternative: RecordNameStrategy → subject = "{record-namespace}.{record-name}"
///   The training uses TopicNameStrategy (one schema type per topic).
/// </summary>
public sealed class AvroMedicinalProductProducer : IAsyncDisposable
{
    private readonly IProducer<string, MedicinalProduct> _producer;
    private readonly KafkaOptions _options;
    private readonly ILogger<AvroMedicinalProductProducer> _logger;

    public AvroMedicinalProductProducer(
        ISchemaRegistryClient schemaRegistryClient,
        IOptions<KafkaOptions> options,
        ILogger<AvroMedicinalProductProducer> logger)
    {
        _options = options.Value;
        _logger = logger;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers    = _options.BootstrapServers,
            SecurityProtocol    = SecurityProtocol.SaslSsl,
            SaslMechanism       = SaslMechanism.Plain,
            SaslUsername        = _options.ApiKey,
            SaslPassword        = _options.ApiSecret,
            ClientId            = "hci-avro-producer",
            Acks                = Acks.All,
            EnableIdempotence   = true,
            MaxInFlight         = 5,
            LingerMs            = 5,
            BatchSize           = 65536,
            CompressionType     = CompressionType.Lz4,
            MessageTimeoutMs    = 30000,
        };

        // ── AvroSerializerConfig ───────────────────────────────────────────────
        var avroConfig = new AvroSerializerConfig
        {
            AutoRegisterSchemas = false,
            // SubjectNameStrategy: TopicNameStrategy is the default.
            // The subject name will be "{topic}-value" (e.g. "hci.medicinal-products.avro-value")
            SubjectNameStrategy = SubjectNameStrategy.Topic,
            // BufferBytes: Size of the internal write buffer. Default 128 bytes.
            // Increase for large messages (e.g. products with long descriptions).
            BufferBytes = 512,
        };

        _producer = new ProducerBuilder<string, MedicinalProduct>(producerConfig)
            .SetValueSerializer(new AvroSerializer<MedicinalProduct>(schemaRegistryClient, avroConfig).AsSyncOverAsync())
            // String key uses the default Confluent UTF-8 string serializer
            .SetErrorHandler((_, error) =>
            {
                if (error.IsFatal)
                    logger.LogCritical("[FATAL] Producer error: {Code} - {Reason}", error.Code, error.Reason);
                else
                    logger.LogError("Producer error: {Code} - {Reason}", error.Code, error.Reason);
            })
            .SetLogHandler((_, msg) =>
                logger.LogDebug("[librdkafka:{Facility}] {Message}", msg.Facility, msg.Message))
            .Build();

        logger.LogInformation(
            "AvroMedicinalProductProducer built. Topic={Topic} AutoRegister={AutoRegister}",
            _options.MedicinalProductsTopic, avroConfig.AutoRegisterSchemas);
    }

    /// <summary>
    /// Produces a single MedicinalProduct using async produce (awaits delivery confirmation).
    /// Use for low-throughput, high-reliability scenarios where ordering confirmation matters.
    /// </summary>
    public async Task<DeliveryResult<string, MedicinalProduct>> ProduceAsync(
        MedicinalProduct product,
        CancellationToken ct = default)
    {
        var message = new Message<string, MedicinalProduct>
        {
            Key   = product.Gtin,       // Partition key — same GTIN always goes to same partition
            Value = product,
            Headers = BuildHeaders(product)
        };

        try
        {
            var result = await _producer.ProduceAsync(_options.MedicinalProductsTopic, message, ct);

            _logger.LogDebug(
                "Produced GTIN={Gtin} status={Status} → {Topic}[{Partition}]@{Offset}",
                product.Gtin, product.MarketingStatus,
                result.Topic, result.Partition.Value, result.Offset.Value);

            return result;
        }
        catch (ProduceException<string, MedicinalProduct> ex)
        {
            _logger.LogError(ex,
                "Failed to produce GTIN={Gtin}: {Code} fatal={Fatal}",
                product.Gtin, ex.Error.Code, ex.Error.IsFatal);
            throw;
        }
    }

    /// <summary>
    /// Produces a batch using fire-and-forget (non-awaited Produce).
    /// Significantly higher throughput than ProduceAsync for bulk ingestion.
    /// Delivery errors are captured in the callback and logged.
    /// </summary>
    public async Task ProduceBatchAsync(
        IEnumerable<MedicinalProduct> products,
        CancellationToken ct = default)
    {
        var count = 0;
        var errors = 0;

        foreach (var product in products)
        {
            if (ct.IsCancellationRequested) break;

            _producer.Produce(
                _options.MedicinalProductsTopic,
                new Message<string, MedicinalProduct>
                {
                    Key   = product.Gtin,
                    Value = product,
                    Headers = BuildHeaders(product)
                },
                deliveryReport =>
                {
                    if (deliveryReport.Error.IsError)
                    {
                        Interlocked.Increment(ref errors);
                        _logger.LogWarning("Batch delivery error for GTIN={Gtin}: {Error}",
                            product.Gtin, deliveryReport.Error);
                    }
                });

            count++;

            // Yield every 1000 messages to avoid blocking the thread pool
            if (count % 1000 == 0)
            {
                _logger.LogInformation("Batch progress: {Count} messages queued, {Errors} errors", count, errors);
                await Task.Yield();
            }
        }

        // Flush ensures all in-flight messages are delivered before returning
        _producer.Flush(TimeSpan.FromSeconds(60));
        _logger.LogInformation("Batch complete: {Count} messages produced, {Errors} delivery errors", count, errors);
    }

    private static Headers BuildHeaders(MedicinalProduct product)
    {
        var headers = new Headers
        {
            { "schema-version",  "1"u8.ToArray() },
            { "content-type",    "application/avro"u8.ToArray() },
            { "data-source",     System.Text.Encoding.UTF8.GetBytes(product.DataSource) },
            { "marketing-status",System.Text.Encoding.UTF8.GetBytes(product.MarketingStatus) },
            { "event-time",      System.Text.Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O")) },
        };
        return headers;
    }

    public async ValueTask DisposeAsync()
    {
        _logger.LogInformation("Flushing Avro producer...");
        _producer.Flush(TimeSpan.FromSeconds(30));
        _producer.Dispose();
        await Task.CompletedTask;
    }
}
