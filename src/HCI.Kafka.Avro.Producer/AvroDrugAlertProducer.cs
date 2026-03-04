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
/// Avro producer for DrugAlert events. 
/// Drug alerts are on the patient-safety critical path — every message must
/// be produced with Acks.All and no data loss is acceptable.
///
/// KEY CONFIGURATION DIFFERENCES vs MedicinalProduct producer:
///   - MessageTimeoutMs = 60s (double — alert must eventually be delivered)
///   - LingerMs = 0           (no batching delay — alerts go immediately)
///   - AutoRegisterSchemas = false (alert schema must be pre-registered — no surprises)
/// </summary>
public sealed class AvroDrugAlertProducer : IAsyncDisposable
{
    private readonly IProducer<string, DrugAlert> _producer;
    private readonly KafkaOptions _options;
    private readonly ILogger<AvroDrugAlertProducer> _logger;

    public AvroDrugAlertProducer(
        ISchemaRegistryClient schemaRegistryClient,
        IOptions<KafkaOptions> options,
        ILogger<AvroDrugAlertProducer> logger)
    {
        _options = options.Value;
        _logger = logger;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers  = _options.BootstrapServers,
            SecurityProtocol  = SecurityProtocol.SaslSsl,
            SaslMechanism     = SaslMechanism.Plain,
            SaslUsername      = _options.ApiKey,
            SaslPassword      = _options.ApiSecret,
            ClientId          = "hci-avro-alert-producer",
            Acks              = Acks.All,
            EnableIdempotence = true,
            MaxInFlight       = 5,
            // CRITICAL DIFFERENCE: no linger delay for safety alerts
            LingerMs          = 0,
            CompressionType   = CompressionType.Lz4,
            // Allow more time for critical alerts to be delivered
            MessageTimeoutMs  = 60000,
        };

        var avroConfig = new AvroSerializerConfig
        {
            // PRODUCTION SAFETY: Schema must exist before we produce any alert.
            // If the schema isn't registered, ProduceAsync will throw immediately,
            // rather than silently registering an incorrect schema.
            AutoRegisterSchemas = false,
            SubjectNameStrategy = SubjectNameStrategy.Topic,
        };

        _producer = new ProducerBuilder<string, DrugAlert>(producerConfig)
            .SetValueSerializer(new AvroSerializer<DrugAlert>(schemaRegistryClient, avroConfig).AsSyncOverAsync())
            .SetErrorHandler((_, error) =>
            {
                // DrugAlert errors are always at least LogError level
                logger.LogError("[DrugAlert Producer] {Fatal} error: {Code} - {Reason}",
                    error.IsFatal ? "FATAL" : "Non-fatal", error.Code, error.Reason);
            })
            .Build();

        logger.LogInformation(
            "AvroDrugAlertProducer built. Topic={Topic} AutoRegister=false (safety mode)",
            _options.DrugAlertsTopic);
    }

    /// <summary>
    /// Produces a DrugAlert. Always awaits delivery confirmation — fire-and-forget
    /// is not acceptable for patient-safety events.
    /// </summary>
    public async Task<DeliveryResult<string, DrugAlert>> ProduceAsync(
        DrugAlert alert,
        CancellationToken ct = default)
    {
        _logger.LogInformation(
            "Producing DrugAlert: alertId={AlertId} gtin={Gtin} type={Type} severity={Severity}",
            alert.AlertId, alert.Gtin, alert.AlertType, alert.Severity);

        var message = new Message<string, DrugAlert>
        {
            Key   = alert.Gtin,
            Value = alert,
            Headers = new Headers
            {
                { "alert-type",     System.Text.Encoding.UTF8.GetBytes(alert.AlertType) },
                { "severity",       System.Text.Encoding.UTF8.GetBytes(alert.Severity) },
                { "schema-version", "1"u8.ToArray() },
                { "content-type",   "application/avro"u8.ToArray() },
            }
        };

        var result = await _producer.ProduceAsync(_options.DrugAlertsTopic, message, ct);

        _logger.LogInformation(
            "DrugAlert delivered: alertId={AlertId} → {Topic}[{Partition}]@{Offset}",
            alert.AlertId, result.Topic, result.Partition.Value, result.Offset.Value);

        return result;
    }

    public async ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(30));
        _producer.Dispose();
        await Task.CompletedTask;
    }
}
