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
/// Avro producer for PriceUpdate events demonstrating BACKWARD schema compatibility.
///
/// BACKWARD COMPATIBILITY LAB (Lab 3D):
/// ─────────────────────────────────────────────────────────────────────────────
/// The PriceUpdate schema v1.1 adds the optional "approvalReference" field with
/// a default of null. This is a BACKWARD-compatible change:
///
///   v1 event:   { gtin, prices..., changeReason, changedBy }
///   v1.1 event: { gtin, prices..., changeReason, changedBy, approvalReference: "BAG-12345" }
///
/// A consumer using the v1 schema can read v1.1 events — it simply ignores the
/// new field. This is the core promise of BACKWARD compatibility.
///
/// WHAT WOULD BE BACKWARD INCOMPATIBLE:
///   - Removing a required field (old consumers expect it)
///   - Renaming a field (Avro identifies fields by name, not position)
///   - Changing a field type (e.g. double → string)
///   - Adding a REQUIRED field without a default
/// </summary>
public sealed class AvroPriceUpdateProducer : IAsyncDisposable
{
    private readonly IProducer<string, PriceUpdate> _producer;
    private readonly KafkaOptions _options;
    private readonly ILogger<AvroPriceUpdateProducer> _logger;

    public AvroPriceUpdateProducer(
        ISchemaRegistryClient schemaRegistryClient,
        IOptions<KafkaOptions> options,
        ILogger<AvroPriceUpdateProducer> logger)
    {
        _options = options.Value;
        _logger = logger;

        _producer = new ProducerBuilder<string, PriceUpdate>(new ProducerConfig
        {
            BootstrapServers  = _options.BootstrapServers,
            SecurityProtocol  = SecurityProtocol.SaslSsl,
            SaslMechanism     = SaslMechanism.Plain,
            SaslUsername      = _options.ApiKey,
            SaslPassword      = _options.ApiSecret,
            ClientId          = "hci-avro-price-producer",
            Acks              = Acks.All,
            EnableIdempotence = true,
        })
        .SetValueSerializer(new AvroSerializer<PriceUpdate>(schemaRegistryClient, new AvroSerializerConfig
        {
            AutoRegisterSchemas = false,
            SubjectNameStrategy = SubjectNameStrategy.Topic,
        }).AsSyncOverAsync())
        .Build();
    }

    public async Task<DeliveryResult<string, PriceUpdate>> ProduceAsync(
        PriceUpdate priceUpdate,
        CancellationToken ct = default)
    {
        var result = await _producer.ProduceAsync(
            _options.PriceUpdatesTopic,
            new Message<string, PriceUpdate>
            {
                Key   = priceUpdate.Gtin,
                Value = priceUpdate,
                Headers = new Headers
                {
                    { "schema-version", priceUpdate.ApprovalReference is null ? "1.0"u8.ToArray() : "1.1"u8.ToArray() },
                    { "content-type",   "application/avro"u8.ToArray() },
                }
            },
            ct);

        _logger.LogDebug(
            "PriceUpdate produced: GTIN={Gtin} approvalRef={Ref} → {Partition}@{Offset}",
            priceUpdate.Gtin,
            priceUpdate.ApprovalReference ?? "null",
            result.Partition.Value,
            result.Offset.Value);

        return result;
    }

    public async ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(30));
        _producer.Dispose();
        await Task.CompletedTask;
    }
}
