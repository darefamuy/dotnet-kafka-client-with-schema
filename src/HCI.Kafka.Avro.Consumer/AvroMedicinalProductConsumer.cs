using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using HCI.Kafka.Avro.Contracts;
using HCI.Kafka.Avro.SchemaRegistry.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HCI.Kafka.Avro.Consumer;

/// <summary>
/// Avro consumer for MedicinalProduct and DrugAlert events.
///
/// HOW AVRO DESERIALISATION WORKS END-TO-END
/// ─────────────────────────────────────────────────────────────────────────────
/// 1. Consumer receives raw bytes from Kafka
/// 2. AvroDeserializer reads byte[0] = 0x00 (magic byte — confirms this is SR format)
/// 3. AvroDeserializer reads bytes[1..4] = schema ID (e.g. 42)
/// 4. AvroDeserializer calls CachedSchemaRegistryClient.GetSchemaAsync(42)
///    - Cache HIT:  returns schema immediately (no network call)
///    - Cache MISS: fetches from Schema Registry, caches for future messages
/// 5. AvroDeserializer uses the writer schema (from SR) + reader schema (MedicinalProduct.Schema)
///    to perform schema resolution (handles field additions/removals per compatibility rules)
/// 6. Returns a populated MedicinalProduct instance
///
/// WRITER SCHEMA vs READER SCHEMA
/// ─────────────────────────────────────────────────────────────────────────────
/// - Writer schema: the schema the producer used to encode the message (from SR by ID)
/// - Reader schema: the schema this consumer has compiled in (MedicinalProduct.Schema)
///
/// Avro schema resolution handles the case where they differ:
///   - Writer has field "approvalReference" (v1.1), reader uses v1.0 schema
///     → The field is silently ignored during deserialisation (BACKWARD compatible)
///
///   - Reader has field "packageSize" with default null, writer's v1.0 didn't have it
///     → The reader fills in the default value (FORWARD compatible reading old data)
///
/// This is the power of Avro schema evolution — consumers and producers can be
/// deployed independently without tight version coupling.
///
/// IMPORTANT: AutoRegisterSchemas on the CONSUMER SIDE
/// ─────────────────────────────────────────────────────────────────────────────
/// AvroDeserializerConfig does NOT have AutoRegisterSchemas — it's read-only.
/// The consumer only looks up schemas by ID, it never registers them.
/// Schemas are only registered by producers (or explicitly via HciSchemaRegistrar).
/// </summary>
public sealed class AvroMedicinalProductConsumer : BackgroundService
{
    private readonly ISchemaRegistryClient _schemaRegistryClient;
    private readonly KafkaOptions _options;
    private readonly ILogger<AvroMedicinalProductConsumer> _logger;

    // Statistics tracked for the lab exercise summary
    private long _consumed;
    private long _active;
    private long _suspended;
    private long _withdrawn;
    private long _pending;

    public AvroMedicinalProductConsumer(
        ISchemaRegistryClient schemaRegistryClient,
        IOptions<KafkaOptions> options,
        ILogger<AvroMedicinalProductConsumer> logger)
    {
        _schemaRegistryClient = schemaRegistryClient;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers      = _options.BootstrapServers,
            SecurityProtocol      = SecurityProtocol.SaslSsl,
            SaslMechanism         = SaslMechanism.Plain,
            SaslUsername          = _options.ApiKey,
            SaslPassword          = _options.ApiSecret,
            ClientId              = "hci-avro-consumer",
            GroupId               = _options.ConsumerGroupId,
            AutoOffsetReset       = AutoOffsetReset.Earliest,
            EnableAutoCommit      = false,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs      = 45000,
            HeartbeatIntervalMs   = 3000,
            MaxPollIntervalMs     = 300000,
        };

        using var consumer = new ConsumerBuilder<string, MedicinalProduct>(consumerConfig)
            .SetValueDeserializer(new AvroDeserializer<MedicinalProduct>(_schemaRegistryClient).AsSyncOverAsync())
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation(
                    "Partitions assigned: [{Partitions}]",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .SetPartitionsRevokedHandler((c, offsets) =>
            {
                _logger.LogWarning("Partitions revoked (rebalance). Committing {Count} offsets.", offsets.Count);
                var valid = offsets.Where(o => o.Offset != Offset.Unset).ToList();
                if (valid.Any())
                    c.Commit(valid);
            })
            .SetErrorHandler((_, error) =>
            {
                if (error.IsFatal)
                    _logger.LogCritical("[FATAL] Consumer error: {Code} {Reason}", error.Code, error.Reason);
                else
                    _logger.LogWarning("Consumer error: {Code} {Reason}", error.Code, error.Reason);
            })
            .Build();

        consumer.Subscribe(_options.MedicinalProductsTopic);
        _logger.LogInformation(
            "Avro consumer started. Group={Group} Topic={Topic}",
            _options.ConsumerGroupId, _options.MedicinalProductsTopic);

        try
        {
            await ConsumeLoopAsync(consumer, stoppingToken);
        }
        finally
        {
            _logger.LogInformation("Consumer closing. Total consumed: {Count}", _consumed);
            LogSummary();
            consumer.Close();
        }
    }

    private async Task ConsumeLoopAsync(
        IConsumer<string, MedicinalProduct> consumer,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, MedicinalProduct>? result = null;
            try
            {
                result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is null || result.IsPartitionEOF) continue;

                await ProcessProductAsync(result.Message.Value, result);
                consumer.StoreOffset(result);
                consumer.Commit(result);
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
            {
                // ── SCHEMA RESOLUTION ERROR ──────────────────────────────────
                // This happens when:
                //   1. The writer used an incompatible schema version
                //   2. The message is not Avro (e.g. a stray JSON message in the topic)
                //   3. The schema ID in the message doesn't exist in Schema Registry
                //
                // For HCI: this means corrupted data or a producer bug.
                // Route to DLQ (covered in Module 7).
                _logger.LogError(ex,
                    "Avro deserialisation failed at {Topic}[{Partition}]@{Offset}. " +
                    "Message may be malformed or written with an incompatible schema. " +
                    "In Module 7 this message would be routed to the DLQ.",
                    result?.Topic, result?.Partition.Value, result?.Offset.Value);

                // Skip the undeserializable message and commit past it
                if (result is not null)
                {
                    consumer.StoreOffset(result);
                    consumer.Commit(result);
                }
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                _logger.LogWarning(ex, "Transient consume error: {Code}", ex.Error.Code);
                await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private Task ProcessProductAsync(
        MedicinalProduct product,
        ConsumeResult<string, MedicinalProduct> result)
    {
        Interlocked.Increment(ref _consumed);

        // Track status distribution for the summary
        switch (product.MarketingStatus)
        {
            case "ACTIVE":    Interlocked.Increment(ref _active);    break;
            case "SUSPENDED": Interlocked.Increment(ref _suspended); break;
            case "WITHDRAWN": Interlocked.Increment(ref _withdrawn); break;
            case "PENDING":   Interlocked.Increment(ref _pending);   break;
        }

        // Flag urgent statuses
        if (product.MarketingStatus is "SUSPENDED" or "WITHDRAWN")
        {
            _logger.LogWarning(
                "⚠  Status change: GTIN={Gtin} Product={Name} Status={Status} Source={Source}",
                product.Gtin, product.ProductName, product.MarketingStatus, product.DataSource);
        }
        else
        {
            _logger.LogInformation(
                "✓ Consumed: GTIN={Gtin} [{ATC}] {Name} CHF {Price:F2} → {Topic}[{Partition}]@{Offset}",
                product.Gtin, product.AtcCode, product.ProductName, product.PublicPriceCHF,
                result.Topic, result.Partition.Value, result.Offset.Value);
        }

        return Task.CompletedTask;
    }

    private void LogSummary()
    {
        _logger.LogInformation(
            "═══ Consumer Summary ═══════════════════════════════\n" +
            "  Total consumed : {Total}\n" +
            "  ACTIVE         : {Active}\n" +
            "  PENDING        : {Pending}\n" +
            "  SUSPENDED      : {Suspended}  ← Requires downstream notification\n" +
            "  WITHDRAWN      : {Withdrawn}  ← Requires urgent downstream action\n" +
            "════════════════════════════════════════════════════",
            _consumed, _active, _pending, _suspended, _withdrawn);
    }
}
