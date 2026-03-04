namespace HCI.Kafka.Avro.Contracts;

/// <summary>
/// Canonical Avro schema JSON strings for all HCI event types.
/// These must exactly match the .avsc files to ensure fingerprint stability.
/// </summary>
public static class SchemaDefinitions
{
    public const string MedicinalProductSchemaJson = """
    {
      "type": "record",
      "name": "MedicinalProduct",
      "namespace": "hci",
      "doc": "A medicinal product catalogued by HCI Solutions from Swissmedic, BAG, and manufacturer sources.",
      "fields": [
        { "name": "gtin",                "type": "string",  "doc": "14-digit GS1 GTIN" },
        { "name": "authorizationNumber", "type": "string",  "doc": "Swissmedic authorisation number" },
        { "name": "productName",         "type": "string" },
        { "name": "atcCode",             "type": "string" },
        { "name": "dosageForm",          "type": "string" },
        { "name": "manufacturer",        "type": "string" },
        { "name": "activeSubstance",     "type": "string" },
        {
          "name": "marketingStatus",
          "type": { "type": "enum", "name": "MarketingStatus", "symbols": ["ACTIVE","SUSPENDED","WITHDRAWN","PENDING"] },
          "default": "ACTIVE"
        },
        { "name": "exFactoryPriceCHF",  "type": "double" },
        { "name": "publicPriceCHF",     "type": "double" },
        { "name": "lastUpdatedUtc",     "type": {"type":"long","logicalType":"timestamp-millis"} },
        { "name": "dataSource",         "type": "string" },
        { "name": "packageSize",        "type": ["null","string"], "default": null },
        { "name": "narcoticsCategory",  "type": ["null","string"], "default": null }
      ]
    }
    """;

    public const string DrugAlertSchemaJson = """
    {
      "type": "record",
      "name": "DrugAlert",
      "namespace": "hci",
      "fields": [
        { "name": "alertId",     "type": "string" },
        { "name": "gtin",        "type": "string" },
        { "name": "alertType",   "type": { "type": "enum", "name": "AlertType",     "symbols": ["RECALL","SHORTAGE","QUALITY_DEFECT","PRICE_SUSPENSION","IMPORT_BAN"] } },
        { "name": "severity",    "type": { "type": "enum", "name": "AlertSeverity", "symbols": ["CLASS_I","CLASS_II","CLASS_III","ADVISORY"] } },
        { "name": "headline",    "type": "string" },
        { "name": "description", "type": "string" },
        { "name": "affectedLots","type": {"type":"array","items":"string"}, "default": [] },
        { "name": "issuedByUtc", "type": {"type":"long","logicalType":"timestamp-millis"} },
        { "name": "expiresUtc",  "type": ["null",{"type":"long","logicalType":"timestamp-millis"}], "default": null },
        { "name": "sourceUrl",   "type": "string" }
      ]
    }
    """;

    public const string PriceUpdateSchemaJson = """
    {
      "type": "record",
      "name": "PriceUpdate",
      "namespace": "hci",
      "fields": [
        { "name": "gtin",                       "type": "string" },
        { "name": "previousExFactoryPriceCHF",  "type": "double" },
        { "name": "newExFactoryPriceCHF",        "type": "double" },
        { "name": "previousPublicPriceCHF",      "type": "double" },
        { "name": "newPublicPriceCHF",           "type": "double" },
        { "name": "effectiveDateUtc",            "type": {"type":"long","logicalType":"timestamp-millis"} },
        { "name": "changeReason",                "type": "string" },
        { "name": "changedBy",                   "type": "string" },
        { "name": "approvalReference",           "type": ["null","string"], "default": null }
      ]
    }
    """;
}
