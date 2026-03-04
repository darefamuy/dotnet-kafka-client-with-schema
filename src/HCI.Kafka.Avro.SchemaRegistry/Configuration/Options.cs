namespace HCI.Kafka.Avro.SchemaRegistry.Configuration;

/// <summary>Options for Confluent Cloud Kafka broker connection.</summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";
    public required string BootstrapServers { get; init; }
    public required string ApiKey { get; init; }
    public required string ApiSecret { get; init; }
    public string MedicinalProductsTopic { get; init; } = "hci.medicinal-products.avro";
    public string DrugAlertsTopic { get; init; } = "hci.drug-alerts.avro";
    public string PriceUpdatesTopic { get; init; } = "hci.price-updates.avro";
    public string ConsumerGroupId { get; init; } = "hci-avro-consumer";
    public int ProduceCount { get; init; } = 100;
}

/// <summary>Options for Confluent Schema Registry connection.</summary>
public sealed class SchemaRegistryOptions
{
    public const string SectionName = "SchemaRegistry";
    public required string Url { get; init; }
    public required string ApiKey { get; init; }
    public required string ApiSecret { get; init; }

    /// <summary>
    /// Maximum number of schemas cached locally. Each schema fetch is a network call —
    /// caching is critical for performance. Default 1000 covers all HCI schema versions.
    /// </summary>
    public int MaxCachedSchemas { get; init; } = 1000;

    /// <summary>
    /// How long to cache schema lookups. Increase in production to reduce SR load.
    /// 5 minutes balances freshness with performance.
    /// </summary>
    public int CacheLatestTtlSecs { get; init; } = 300;
}
