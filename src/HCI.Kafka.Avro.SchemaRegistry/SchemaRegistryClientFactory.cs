using Confluent.SchemaRegistry;
using HCI.Kafka.Avro.SchemaRegistry.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HCI.Kafka.Avro.SchemaRegistry;

/// <summary>
/// Factory for creating a configured <see cref="CachedSchemaRegistryClient"/>.
///
/// WHY CachedSchemaRegistryClient?
/// ─────────────────────────────────────────────────────────────────────────────
/// Every Avro message serialisation/deserialisation requires a schema lookup:
///   - Serialize: look up schema ID by schema fingerprint → embed ID in message wire format
///   - Deserialize: look up schema by the 4-byte schema ID embedded in the message
///
/// Without caching, every message would make a network call to Schema Registry —
/// at 10,000 msg/s that is 10,000 HTTP requests per second.
///
/// CachedSchemaRegistryClient caches:
///   - Schema → ID mappings  (for producers: avoids duplicate registrations)
///   - ID → Schema mappings  (for consumers: avoids repeated lookups)
///
/// Cache invalidation: Schemas are immutable once registered — a schema ID always
/// refers to the same schema forever. The cache never needs to be invalidated for
/// existing schema IDs. Only the "latest version" pointer can change.
///
/// WIRE FORMAT REMINDER (Confluent Schema Registry wire format):
/// ─────────────────────────────────────────────────────────────────────────────
/// Each Avro message serialised by Confluent's AvroSerializer has the format:
///
///   Byte 0:     Magic byte = 0x00 (schema registry protocol marker)
///   Bytes 1-4:  Schema ID (big-endian int32)
///   Bytes 5+:   Avro-encoded payload
///
/// This is why consumers need the Schema Registry: to look up the schema by ID
/// before they can decode the Avro bytes.
/// </summary>
public static class SchemaRegistryClientFactory
{
    /// <summary>
    /// Creates a production-configured CachedSchemaRegistryClient for Confluent Cloud.
    /// The returned client is thread-safe and should be registered as a singleton.
    /// </summary>
    public static CachedSchemaRegistryClient Create(
        SchemaRegistryOptions options,
        ILogger? logger = null)
    {
        var config = new SchemaRegistryConfig
        {
            Url = options.Url,
            BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
            BasicAuthUserInfo = $"{options.ApiKey}:{options.ApiSecret}",

            // ── Cache configuration ────────────────────────────────────────
            // MaxCachedSchemas: maximum number of schema ID → schema mappings held in memory.
            // 1000 covers all versions of all HCI schemas for the foreseeable future.
            MaxCachedSchemas = options.MaxCachedSchemas,

            // ── SSL ────────────────────────────────────────────────────────
            // Confluent Cloud Schema Registry is always HTTPS.
            // No additional SSL cert config needed when using the managed service.
        };

        logger?.LogInformation(
            "Creating CachedSchemaRegistryClient: url={Url} maxCache={MaxCache}",
            options.Url, options.MaxCachedSchemas);

        return new CachedSchemaRegistryClient(config);
    }

    /// <summary>Creates a client from IOptions — for use with Microsoft DI.</summary>
    public static CachedSchemaRegistryClient Create(
        IOptions<SchemaRegistryOptions> options,
        ILogger<CachedSchemaRegistryClient>? logger = null) =>
        Create(options.Value, logger);
}
