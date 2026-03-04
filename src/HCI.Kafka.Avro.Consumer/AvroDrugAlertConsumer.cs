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
/// Avro consumer for DrugAlert events.
///
/// TRAINING NOTE: This consumer demonstrates how to read the "severity" header
/// for fast-path routing WITHOUT deserialising the Avro payload. For CLASS_I
/// alerts, hospitals may want to forward the raw alert to an emergency pager
/// system before the full payload is even decoded.
///
/// This pattern is useful because:
///   - Avro deserialisation requires a Schema Registry lookup (even if cached, it costs CPU)
///   - Headers are available immediately with O(1) cost
///   - For emergency routing, latency matters more than full payload processing
/// </summary>
public sealed class AvroDrugAlertConsumer : BackgroundService
{
    private readonly ISchemaRegistryClient _schemaRegistryClient;
    private readonly KafkaOptions _options;
    private readonly ILogger<AvroDrugAlertConsumer> _logger;

    public AvroDrugAlertConsumer(
        ISchemaRegistryClient schemaRegistryClient,
        IOptions<KafkaOptions> options,
        ILogger<AvroDrugAlertConsumer> logger)
    {
        _schemaRegistryClient = schemaRegistryClient;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers      = _options.BootstrapServers,
            SecurityProtocol      = SecurityProtocol.SaslSsl,
            SaslMechanism         = SaslMechanism.Plain,
            SaslUsername          = _options.ApiKey,
            SaslPassword          = _options.ApiSecret,
            ClientId              = "hci-avro-alert-consumer",
            GroupId               = $"{_options.ConsumerGroupId}-alerts",
            AutoOffsetReset       = AutoOffsetReset.Earliest,
            EnableAutoCommit      = false,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs      = 45000,
            HeartbeatIntervalMs   = 3000,
        };

        using var consumer = new ConsumerBuilder<string, DrugAlert>(config)
            .SetValueDeserializer(
                new AvroDeserializer<DrugAlert>(_schemaRegistryClient).AsSyncOverAsync())
            .SetPartitionsRevokedHandler((c, offsets) =>
            {
                _logger.LogWarning("Alert consumer partitions revoked. Committing offsets.");
                var valid = offsets.Where(o => o.Offset != Offset.Unset).ToList();
                if (valid.Any()) c.Commit(valid);
            })
            .Build();

        consumer.Subscribe(_options.DrugAlertsTopic);
        _logger.LogInformation("DrugAlert Avro consumer started. Topic={Topic}", _options.DrugAlertsTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is null || result.IsPartitionEOF) continue;

                // ── Fast-path header check BEFORE Avro deserialisation ────────
                // Read the severity header to enable immediate emergency routing
                var severityHeader = result.Message.Headers
                    .FirstOrDefault(h => h.Key == "severity");

                if (severityHeader is not null)
                {
                    var severity = System.Text.Encoding.UTF8.GetString(severityHeader.GetValueBytes());
                    if (severity == "CLASS_I")
                    {
                        // In production: trigger emergency pager before full processing
                        _logger.LogCritical(
                            "🚨 CLASS_I ALERT detected via header at {Topic}[{Partition}]@{Offset}. " +
                            "Initiating emergency notification pipeline...",
                            result.Topic, result.Partition.Value, result.Offset.Value);
                    }
                }

                // ── Full Avro deserialization for complete alert processing ────
                var alert = result.Message.Value;
                ProcessAlert(alert, result);

                consumer.StoreOffset(result);
                consumer.Commit(result);
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                _logger.LogWarning(ex, "DrugAlert consumer error: {Code}", ex.Error.Code);
                await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        consumer.Close();
    }

    private void ProcessAlert(DrugAlert alert, ConsumeResult<string, DrugAlert> result)
    {
        var emoji = alert.Severity switch
        {
            "CLASS_I"  => "🚨",
            "CLASS_II" => "⚠️",
            "CLASS_III"=> "ℹ️",
            _          => "📋"
        };

        _logger.LogInformation(
            "{Emoji} DrugAlert consumed: alertId={AlertId} type={Type} severity={Severity} " +
            "gtin={Gtin} lots=[{Lots}] → [{Partition}]@{Offset}",
            emoji,
            alert.AlertId,
            alert.AlertType,
            alert.Severity,
            alert.Gtin,
            string.Join(", ", alert.AffectedLots),
            result.Partition.Value,
            result.Offset.Value);
    }
}
