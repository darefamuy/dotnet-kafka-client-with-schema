using HCI.Kafka.Avro.Contracts;
using HCI.Kafka.Avro.SchemaRegistry;
using HCI.Kafka.Avro.SchemaRegistry.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HCI.Kafka.Avro.Producer;

/// <summary>
/// BackgroundService that orchestrates the Avro producer demonstration.
///
/// SEQUENCE:
///   1. Register all schemas with Schema Registry (idempotent)
///   2. Print schema details (versions, IDs, compatibility)
///   3. Produce a batch of MedicinalProduct events
///   4. Produce several DrugAlert events
///   5. Produce PriceUpdate events (demonstrates backward compatibility)
///   6. Print throughput statistics
/// </summary>
public sealed class ProducerWorker : BackgroundService
{
    private readonly AvroMedicinalProductProducer _productProducer;
    private readonly AvroDrugAlertProducer _alertProducer;
    private readonly AvroPriceUpdateProducer _priceProducer;
    private readonly HciSchemaRegistrar _schemaRegistrar;
    private readonly KafkaOptions _options;
    private readonly ILogger<ProducerWorker> _logger;
    private readonly IHostApplicationLifetime _lifetime;

    public ProducerWorker(
        AvroMedicinalProductProducer productProducer,
        AvroDrugAlertProducer alertProducer,
        AvroPriceUpdateProducer priceProducer,
        HciSchemaRegistrar schemaRegistrar,
        IOptions<KafkaOptions> options,
        IHostApplicationLifetime lifetime,
        ILogger<ProducerWorker> logger)
    {
        _productProducer  = productProducer;
        _alertProducer    = alertProducer;
        _priceProducer    = priceProducer;
        _schemaRegistrar  = schemaRegistrar;
        _options          = options.Value;
        _lifetime         = lifetime;
        _logger           = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        try
        {
            // ── Step 1: Produce MedicinalProduct batch ────────────────────────
            _logger.LogInformation("═══ STEP 3: Producing {Count} MedicinalProducts ═══", _options.ProduceCount);
            var products = SampleDataFactory.GenerateBatch(_options.ProduceCount);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            await _productProducer.ProduceBatchAsync(products, ct);
            sw.Stop();

            _logger.LogInformation(
                "✓ Produced {Count} MedicinalProduct Avro messages in {ElapsedMs}ms " +
                "({Rate:F0} msg/s)",
                _options.ProduceCount, sw.ElapsedMilliseconds,
                _options.ProduceCount / (sw.ElapsedMilliseconds / 1000.0));

            // ── Step 2: Produce DrugAlerts ────────────────────────────────────
            _logger.LogInformation("═══ STEP 4: Producing DrugAlerts ══════════════");
            var alertGtins = products.Take(5).Select(p => p.Gtin).ToList();
            foreach (var gtin in alertGtins)
            {
                var alert = SampleDataFactory.RandomAlert(gtin);
                await _alertProducer.ProduceAsync(alert, ct);
            }
            _logger.LogInformation("✓ Produced {Count} DrugAlert Avro messages", alertGtins.Count);

            // ── Step 3: Produce PriceUpdates (backward compatibility demo) ────
            _logger.LogInformation("═══ STEP 5: Producing PriceUpdates ════════════");
            foreach (var product in products.Take(10))
            {
                // First batch: v1 events (no approvalReference)
                var priceV1 = SampleDataFactory.RandomPriceUpdate(product.Gtin, includeApprovalRef: false);
                await _priceProducer.ProduceAsync(priceV1, ct);

                // Second batch: v1.1 events (with approvalReference — backward compatible)
                var priceV11 = SampleDataFactory.RandomPriceUpdate(product.Gtin, includeApprovalRef: true);
                await _priceProducer.ProduceAsync(priceV11, ct);
            }
            _logger.LogInformation("✓ Produced PriceUpdate events (v1 and v1.1 formats)");

            // ── Step 4: Summary ───────────────────────────────────────────────
            _logger.LogInformation("═══ STEP 6: Production Summary ════════════════");
            _logger.LogInformation(
                "All messages produced successfully.\n" +
                "  MedicinalProducts : {ProductCount}\n" +
                "  DrugAlerts        : {AlertCount}\n" +
                "  PriceUpdates      : {PriceCount}\n" +
                "\nRun the consumer: dotnet run --project src/HCI.Kafka.Avro.Consumer",
                _options.ProduceCount,
                alertGtins.Count,
                20);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "ProducerWorker failed");
        }
        finally
        {
            _lifetime.StopApplication();
        }
    }
}
