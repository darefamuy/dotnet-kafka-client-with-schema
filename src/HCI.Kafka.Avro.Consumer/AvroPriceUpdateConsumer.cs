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
/// Avro consumer for PriceUpdate events demonstrating BACKWARD compatibility in action.
///
/// LAB 3D — SCHEMA EVOLUTION DEMONSTRATION:
/// ─────────────────────────────────────────────────────────────────────────────
/// This consumer reads from hci.price-updates.s.avro which contains both:
///   - v1.0 events: missing "approvalReference" field
///   - v1.1 events: containing "approvalReference" field
///
/// The consumer uses the v1.1 schema (which includes the new field with default null).
/// When it reads a v1.0 event (writer schema has no "approvalReference"),
/// Avro schema resolution fills in the default value (null).
///
/// This is BACKWARD compatibility working transparently:
///   ✓ Old v1.0 events are readable by the v1.1 schema consumer
///   ✓ No migration needed — old data is still valid
///   ✓ The consumer detects whether it read a v1.0 or v1.1 event by checking
///     whether ApprovalReference is null
/// </summary>
public sealed class AvroPriceUpdateConsumer : BackgroundService
{
    private readonly ISchemaRegistryClient _schemaRegistryClient;
    private readonly KafkaOptions _options;
    private readonly ILogger<AvroPriceUpdateConsumer> _logger;

    public AvroPriceUpdateConsumer(
        ISchemaRegistryClient schemaRegistryClient,
        IOptions<KafkaOptions> options,
        ILogger<AvroPriceUpdateConsumer> logger)
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
            ClientId              = "hci-avro-price-consumer",
            GroupId               = $"{_options.ConsumerGroupId}-prices",
            AutoOffsetReset       = AutoOffsetReset.Earliest,
            EnableAutoCommit      = false,
            EnableAutoOffsetStore = false,
        };

        using var consumer = new ConsumerBuilder<string, PriceUpdate>(config)
            .SetValueDeserializer(
                new AvroDeserializer<PriceUpdate>(_schemaRegistryClient).AsSyncOverAsync())
            .SetPartitionsRevokedHandler((c, offsets) =>
            {
                var valid = offsets.Where(o => o.Offset != Offset.Unset).ToList();
                if (valid.Any()) c.Commit(valid);
            })
            .Build();

        consumer.Subscribe(_options.PriceUpdatesTopic);
        _logger.LogInformation("PriceUpdate Avro consumer started. Topic={Topic}", _options.PriceUpdatesTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is null || result.IsPartitionEOF) continue;

                var update = result.Message.Value;
                var schemaVersion = ReadSchemaVersionHeader(result.Message.Headers);

                // Calculate price change percentage for alerting
                var changePct = ((update.NewPublicPriceCHF - update.PreviousPublicPriceCHF)
                                  / update.PreviousPublicPriceCHF) * 100;

                var changeDirection = changePct >= 0 ? "▲" : "▼";

                _logger.LogInformation(
                    "💰 PriceUpdate (schema v{SchemaVersion}): GTIN={Gtin} " +
                    "{PreviousPrice:F2} → {NewPrice:F2} CHF ({Direction}{ChangePct:F1}%) " +
                    "reason={Reason} approvalRef={ApprovalRef}",
                    schemaVersion,
                    update.Gtin,
                    update.PreviousPublicPriceCHF,
                    update.NewPublicPriceCHF,
                    changeDirection, Math.Abs(changePct),
                    update.ChangeReason,
                    update.ApprovalReference ?? "(v1.0 event — no approval ref)");

                // Alert on large price increases (>40% suggests SL manipulation)
                if (changePct > 40)
                {
                    _logger.LogWarning(
                        "⚠  Large price increase detected: GTIN={Gtin} +{Pct:F1}% — requires BAG review",
                        update.Gtin, changePct);
                }

                consumer.StoreOffset(result);
                consumer.Commit(result);
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                _logger.LogWarning(ex, "PriceUpdate consumer error: {Code}", ex.Error.Code);
                await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        consumer.Close();
    }

    private static string ReadSchemaVersionHeader(Headers headers)
    {
        var header = headers.FirstOrDefault(h => h.Key == "schema-version");
        return header is not null
            ? System.Text.Encoding.UTF8.GetString(header.GetValueBytes())
            : "unknown";
    }
}
