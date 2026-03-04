using Confluent.SchemaRegistry;
using HCI.Kafka.Avro.Contracts;
using HCI.Kafka.Avro.SchemaRegistry.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HCI.Kafka.Avro.SchemaRegistry;

/// <summary>
/// Manages the lifecycle of all HCI Avro schemas in Confluent Schema Registry.
///
/// RESPONSIBILITIES:
/// ─────────────────────────────────────────────────────────────────────────────
/// 1. RegisterAllSchemasAsync     → Idempotent startup registration
/// 2. SetCompatibilityModesAsync  → Enforce per-subject compatibility rules
/// 3. ValidateCompatibilityAsync  → Pre-flight check before deploying new schema version
/// 4. GetSchemaDetailsAsync       → Print current registered schema metadata
/// 5. CheckSubjectExistsAsync     → Verify schema is registered before producing
///
/// CONFLUENT SCHEMA REGISTRY FUNDAMENTALS (training reference)
/// ─────────────────────────────────────────────────────────────────────────────
///
/// Subject naming:
///   By default Confluent uses TopicNameStrategy:
///     - Value schema subject: "{topic-name}-value"
///     - Key schema subject:   "{topic-name}-key"
///   Example: topic "hci.medicinal-products.avro" → subject "hci.medicinal-products.avro-value"
///
/// Compatibility modes:
///   BACKWARD  → New schema can read data written with old schema
///               Safe when adding fields with defaults, removing optional fields.
///               Old consumers can still read new data.
///
///   FORWARD   → Old schema can read data written with new schema
///               Safe when removing fields with defaults, adding optional fields.
///               New consumers can read old data.
///
///   FULL      → Both BACKWARD and FORWARD simultaneously.
///               Most restrictive. Required for cross-team shared contracts.
///               Add fields only with defaults. Never remove or rename fields.
///
///   NONE      → No compatibility checking. DANGEROUS in production.
///               Only acceptable for development or one-off migration topics.
///
/// HCI compatibility decisions:
///   hci.medicinal-products.avro → FULL  (shared with 5+ downstream systems)
///   hci.drug-alerts.avro        → FULL  (patient safety — no breaking changes ever)
///   hci.price-updates.avro   → BACKWARD (BAG SL feed — producers updated first)
/// </summary>
public sealed class HciSchemaRegistrar
{
    private readonly ISchemaRegistryClient _client;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<HciSchemaRegistrar> _logger;

    public HciSchemaRegistrar(
        ISchemaRegistryClient client,
        IOptions<KafkaOptions> kafkaOptions,
        ILogger<HciSchemaRegistrar> logger)
    {
        _client = client;
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
    }

    /// <summary>
    /// Registers all HCI schemas idempotently. Safe to call on every startup.
    /// If the schema already exists and is compatible, registration returns the existing ID.
    /// If the schema is incompatible with the current version, throws SchemaRegistryException.
    /// </summary>
    public async Task RegisterAllSchemasAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("Registering HCI Avro schemas with Confluent Schema Registry...");

        var registrations = new (string subject, string schemaJson, string description)[]
        {
            (
                $"{_kafkaOptions.MedicinalProductsTopic}-value",
                SchemaDefinitions.MedicinalProductSchemaJson,
                "MedicinalProduct"
            ),
            (
                $"{_kafkaOptions.DrugAlertsTopic}-value",
                SchemaDefinitions.DrugAlertSchemaJson,
                "DrugAlert"
            ),
            (
                $"{_kafkaOptions.PriceUpdatesTopic}-value",
                SchemaDefinitions.PriceUpdateSchemaJson,
                "PriceUpdate"
            )
        };

        foreach (var (subject, schemaJson, description) in registrations)
        {
            await RegisterSchemaAsync(subject, schemaJson, description, ct);
        }

        _logger.LogInformation("All HCI schemas registered successfully.");
    }

    /// <summary>
    /// Sets the compatibility mode for each subject.
    /// Call this ONCE when the topic is first created, or when changing compatibility policy.
    ///
    /// WARNING: Changing from FULL to BACKWARD allows breaking changes that
    /// previously would have been rejected. Only change downward under careful review.
    /// </summary>
    public async Task SetCompatibilityModesAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("Setting schema compatibility modes...");

        var modes = new (string subject, Compatibility mode, string rationale)[]
        {
            (
                $"{_kafkaOptions.MedicinalProductsTopic}-value",
                Compatibility.Full,
                "Shared with hospital systems, PharmaVista, Compendium.ch — no breaking changes allowed"
            ),
            (
                $"{_kafkaOptions.DrugAlertsTopic}-value",
                Compatibility.Full,
                "Patient safety topic — maximum compatibility restriction"
            ),
            (
                $"{_kafkaOptions.PriceUpdatesTopic}-value",
                Compatibility.Backward,
                "BAG SL feed — producers updated before consumers, new consumers read old events"
            )
        };

        foreach (var (subject, mode, rationale) in modes)
        {
            try
            {
                await _client.UpdateCompatibilityAsync(mode, subject);
                _logger.LogInformation(
                    "Set {Mode} compatibility for subject={Subject}. Rationale: {Rationale}",
                    mode, subject, rationale);
            }
            catch (SchemaRegistryException ex) when (ex.ErrorCode == 40301)
            {
                _logger.LogWarning(
                    "User is denied operation Write (Compatibility) on Subject: {Subject}. " +
                    "Skipping compatibility update. This must be set by an administrator.",
                    subject);
            }
            catch (SchemaRegistryException ex)
            {
                _logger.LogError(ex,
                    "Failed to set compatibility for {Subject}: {Message}", subject, ex.Message);
                throw;
            }
        }
    }

    /// <summary>
    /// Tests whether a new schema version would be compatible before registering it.
    /// Use this in CI/CD pipelines to catch breaking schema changes early.
    ///
    /// Returns true if compatible, false if the new schema would break the registered rules.
    /// </summary>
    public async Task<bool> ValidateCompatibilityAsync(
        string subject,
        string candidateSchemaJson,
        CancellationToken ct = default)
    {
        _logger.LogInformation("Checking compatibility for subject={Subject}...", subject);

        try
        {
            var isCompatible = await _client.IsCompatibleAsync(
                subject,
                new Schema(candidateSchemaJson, SchemaType.Avro));

            _logger.LogInformation(
                "Compatibility check: subject={Subject} compatible={IsCompatible}",
                subject, isCompatible);

            return isCompatible;
        }
        catch (SchemaRegistryException ex) when (ex.Message.Contains("Subject not found"))
        {
            // No existing schema — any schema is "compatible" (it's the first version)
            _logger.LogInformation(
                "Subject {Subject} not yet registered — first version is always compatible.", subject);
            return true;
        }
    }

    /// <summary>
    /// Retrieves and logs the latest registered schema version and ID for a subject.
    /// Useful for debugging schema version mismatches.
    /// </summary>
    public async Task GetSchemaDetailsAsync(string subject, CancellationToken ct = default)
    {
        try
        {
            var latestSchema = await _client.GetLatestSchemaAsync(subject);
            var versions = await _client.GetSubjectVersionsAsync(subject);
            var compatibility = await _client.GetCompatibilityAsync(subject);

            _logger.LogInformation(
                "Schema details for subject={Subject}:\n" +
                "  Latest version : {Version}\n" +
                "  Schema ID      : {Id}\n" +
                "  All versions   : [{Versions}]\n" +
                "  Compatibility  : {Compatibility}\n" +
                "  Schema preview : {Schema}",
                subject,
                latestSchema.Version,
                latestSchema.Id,
                string.Join(", ", versions),
                compatibility,
                latestSchema.SchemaString.Length > 200
                    ? latestSchema.SchemaString[..200] + "..."
                    : latestSchema.SchemaString);
        }
        catch (SchemaRegistryException ex) when (ex.ErrorCode == 40301)
        {
            _logger.LogWarning(
                "User is denied operation Read on Subject: {Subject}. " +
                "Skipping schema inspection. This might happen if your API key has restricted permissions.",
                subject);
        }
        catch (SchemaRegistryException ex)
        {
            _logger.LogWarning(ex, "Could not fetch schema details for {Subject}", subject);
        }
    }

    /// <summary>
    /// Prints a full schema evolution report — all versions with their IDs.
    /// Useful for lab exercises exploring schema evolution.
    /// </summary>
    public async Task PrintSchemaEvolutionReportAsync(string subject, CancellationToken ct = default)
    {
        _logger.LogInformation("Schema Evolution Report for {Subject}:", subject);
        _logger.LogInformation("═══════════════════════════════════════════");

        try
        {
            var versions = await _client.GetSubjectVersionsAsync(subject);
            foreach (var version in versions.OrderBy(v => v))
            {
                var schema = await _client.GetRegisteredSchemaAsync(subject, version);
                _logger.LogInformation(
                    "  v{Version} → Schema ID: {Id}  ({Length} chars)",
                    version, schema.Id, schema.SchemaString.Length);
            }
        }
        catch (SchemaRegistryException ex)
        {
            _logger.LogWarning(ex, "Could not generate evolution report for {Subject}", subject);
        }

        _logger.LogInformation("═══════════════════════════════════════════");
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private async Task RegisterSchemaAsync(
        string subject,
        string schemaJson,
        string description,
        CancellationToken ct)
    {
        try
        {
            var schemaId = await _client.RegisterSchemaAsync(
                subject,
                new Schema(schemaJson, SchemaType.Avro));

            _logger.LogInformation(
                "Registered {Description} schema: subject={Subject} id={Id}",
                description, subject, schemaId);
        }
        catch (SchemaRegistryException ex) when (ex.ErrorCode == 40301)
        {
            _logger.LogWarning(
                "User is denied operation Write on Subject: {Subject}. " +
                "Skipping registration. The schema must be pre-registered by an administrator.",
                subject);
        }
        catch (SchemaRegistryException ex) when (IsIncompatibleSchemaError(ex))
        {
            _logger.LogCritical(
                "SCHEMA INCOMPATIBILITY DETECTED for {Subject}!\n" +
                "The new schema violates the {Mode} compatibility rules.\n" +
                "This would BREAK existing producers or consumers.\n" +
                "Error: {Message}\n" +
                "Resolution: Review what changed in the schema and either:\n" +
                "  (a) Revert the breaking change\n" +
                "  (b) Create a new topic version (e.g. hci.medicinal-products.v2)\n" +
                "  (c) Use schema migration with a dual-write period",
                subject, "configured", ex.Message);
            throw;
        }
        catch (SchemaRegistryException ex)
        {
            _logger.LogError(ex, "Schema registration failed for {Subject}: {Message}",
                subject, ex.Message);
            throw;
        }
    }

    private static bool IsIncompatibleSchemaError(SchemaRegistryException ex) =>
        ex.Message.Contains("incompatible", StringComparison.OrdinalIgnoreCase) ||
        ex.ErrorCode == 409;
}
