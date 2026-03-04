using Avro;
using Avro.Specific;

namespace HCI.Kafka.Avro.Contracts;

/// <summary>
/// Avro SpecificRecord for a medicinal product catalogued by HCI Solutions.
///
/// HAND-AUTHORED VS CODE-GENERATED
/// ────────────────────────────────────────────────────────────────────────────
/// In production you would typically generate this class from the .avsc schema
/// using the Avro tools: avro-tools.jar compile schema *.avsc ./output/
/// or via the Apache.Avro NuGet task. This class is hand-authored so the
/// training solution is self-contained and the Avro field mappings are
/// clearly visible for learning purposes.
///
/// SCHEMA IDENTITY
/// ────────────────────────────────────────────────────────────────────────────
/// The Schema property MUST exactly match the registered .avsc file.
/// Any discrepancy causes a SchemaRegistryException at serialisation time.
/// The Schema Registry uses the schema fingerprint for compatibility checks.
///
/// SWISS PHARMACEUTICAL DOMAIN FIELDS
/// ────────────────────────────────────────────────────────────────────────────
/// - Gtin:                  14-digit Global Trade Item Number (GS1)
/// - AuthorizationNumber:   Swissmedic authorisation number (e.g. "68177")
/// - AtcCode:               WHO Anatomical Therapeutic Chemical code (e.g. "C09AA05")
/// - ExFactoryPriceCHF:     Manufacturer price in Swiss Francs (before distribution markup)
/// - PublicPriceCHF:        Final pharmacy price (includes 30% distribution margin)
/// - MarketingStatus:       Current regulatory status (ACTIVE/SUSPENDED/WITHDRAWN/PENDING)
/// </summary>
public sealed class MedicinalProduct : ISpecificRecord
{
    private static readonly Schema _schema = Schema.Parse(SchemaDefinitions.MedicinalProductSchemaJson);
    private static readonly string[] MarketingStatusSymbols = { "ACTIVE", "SUSPENDED", "WITHDRAWN", "PENDING" };

    public Schema Schema => _schema;

    // ── Required fields ───────────────────────────────────────────────────────
    public string Gtin { get; set; } = string.Empty;
    public string AuthorizationNumber { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public string AtcCode { get; set; } = string.Empty;
    public string DosageForm { get; set; } = string.Empty;
    public string Manufacturer { get; set; } = string.Empty;
    public string ActiveSubstance { get; set; } = string.Empty;
    public string MarketingStatus { get; set; } = "ACTIVE";
    public double ExFactoryPriceCHF { get; set; }
    public double PublicPriceCHF { get; set; }
    public long LastUpdatedUtc { get; set; }  // timestamp-millis logical type
    public string DataSource { get; set; } = string.Empty;

    // ── Optional fields (nullable with Avro union ["null", "type"]) ───────────
    public string? PackageSize { get; set; }
    public string? NarcoticsCategory { get; set; }

    // ── ISpecificRecord implementation ────────────────────────────────────────

    public object Get(int fieldPos) => fieldPos switch
    {
        0  => Gtin,
        1  => AuthorizationNumber,
        2  => ProductName,
        3  => AtcCode,
        4  => DosageForm,
        5  => Manufacturer,
        6  => ActiveSubstance,
        7  => MarketingStatus,
        8  => ExFactoryPriceCHF,
        9  => PublicPriceCHF,
        10 => DateTimeOffset.FromUnixTimeMilliseconds(LastUpdatedUtc).UtcDateTime,
        11 => DataSource,
        12 => (object?)PackageSize,
        13 => (object?)NarcoticsCategory,
        _  => throw new AvroRuntimeException($"Bad index {fieldPos} in Get()")
    };

    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0:  Gtin                = (string)fieldValue; break;
            case 1:  AuthorizationNumber = (string)fieldValue; break;
            case 2:  ProductName         = (string)fieldValue; break;
            case 3:  AtcCode             = (string)fieldValue; break;
            case 4:  DosageForm          = (string)fieldValue; break;
            case 5:  Manufacturer        = (string)fieldValue; break;
            case 6:  ActiveSubstance     = (string)fieldValue; break;
            case 7:  MarketingStatus     = MarketingStatusSymbols[(int)fieldValue]; break;
            case 8:  ExFactoryPriceCHF   = Convert.ToDouble(fieldValue); break;
            case 9:  PublicPriceCHF      = Convert.ToDouble(fieldValue); break;
            case 10: LastUpdatedUtc      = new DateTimeOffset((DateTime)fieldValue).ToUnixTimeMilliseconds(); break;
            case 11: DataSource          = (string)fieldValue; break;
            case 12: PackageSize         = fieldValue as string; break;
            case 13: NarcoticsCategory   = fieldValue as string; break;
            default: throw new AvroRuntimeException($"Bad index {fieldPos} in Put()");
        }
    }
}

/// <summary>
/// Avro SpecificRecord for a drug safety alert (recall, shortage, quality issue).
/// </summary>
public sealed class DrugAlert : ISpecificRecord
{
    private static readonly Schema _schema = Schema.Parse(SchemaDefinitions.DrugAlertSchemaJson);
    private static readonly string[] AlertTypeSymbols = { "RECALL", "SHORTAGE", "QUALITY_DEFECT", "PRICE_SUSPENSION", "IMPORT_BAN" };
    private static readonly string[] AlertSeveritySymbols = { "CLASS_I", "CLASS_II", "CLASS_III", "ADVISORY" };

    public Schema Schema => _schema;

    public string AlertId { get; set; } = string.Empty;
    public string Gtin { get; set; } = string.Empty;
    public string AlertType { get; set; } = string.Empty;
    public string Severity { get; set; } = string.Empty;
    public string Headline { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public IList<string> AffectedLots { get; set; } = new List<string>();
    public long IssuedByUtc { get; set; }
    public long? ExpiresUtc { get; set; }
    public string SourceUrl { get; set; } = string.Empty;

    public Schema Schema1 => _schema;
    Schema ISpecificRecord.Schema => _schema;

    public object Get(int fieldPos) => fieldPos switch
    {
        0  => AlertId,
        1  => Gtin,
        2  => AlertType,
        3  => Severity,
        4  => Headline,
        5  => Description,
        6  => AffectedLots,
        7  => DateTimeOffset.FromUnixTimeMilliseconds(IssuedByUtc).UtcDateTime,
        8  => (object?)(ExpiresUtc.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(ExpiresUtc.Value).UtcDateTime : null),
        9  => SourceUrl,
        _  => throw new AvroRuntimeException($"Bad index {fieldPos} in Get()")
    };

    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: AlertId     = (string)fieldValue; break;
            case 1: Gtin        = (string)fieldValue; break;
            case 2: AlertType   = AlertTypeSymbols[(int)fieldValue]; break;
            case 3: Severity    = AlertSeveritySymbols[(int)fieldValue]; break;
            case 4: Headline    = (string)fieldValue; break;
            case 5: Description = (string)fieldValue; break;
            case 6: AffectedLots = fieldValue is IList<string> l ? l : new List<string>(); break;
            case 7: IssuedByUtc = new DateTimeOffset((DateTime)fieldValue).ToUnixTimeMilliseconds(); break;
            case 8: ExpiresUtc  = fieldValue is DateTime dt ? new DateTimeOffset(dt).ToUnixTimeMilliseconds() : (long?)null; break;
            case 9: SourceUrl   = (string)fieldValue; break;
            default: throw new AvroRuntimeException($"Bad index {fieldPos} in Put()");
        }
    }
}

/// <summary>
/// Avro SpecificRecord for a price update event — append-only price history.
/// Uses a BACKWARD-compatible schema (new fields have defaults, old fields kept).
/// </summary>
public sealed class PriceUpdate : ISpecificRecord
{
    private static readonly Schema _schema = Schema.Parse(SchemaDefinitions.PriceUpdateSchemaJson);

    public Schema Schema => _schema;

    public string Gtin { get; set; } = string.Empty;
    public double PreviousExFactoryPriceCHF { get; set; }
    public double NewExFactoryPriceCHF { get; set; }
    public double PreviousPublicPriceCHF { get; set; }
    public double NewPublicPriceCHF { get; set; }
    public long EffectiveDateUtc { get; set; }
    public string ChangeReason { get; set; } = string.Empty;
    public string ChangedBy { get; set; } = string.Empty;
    /// <summary>v2 field — added with BACKWARD compatibility. Null in v1 events.</summary>
    public string? ApprovalReference { get; set; }

    public object Get(int fieldPos) => fieldPos switch
    {
        0 => Gtin,
        1 => PreviousExFactoryPriceCHF,
        2 => NewExFactoryPriceCHF,
        3 => PreviousPublicPriceCHF,
        4 => NewPublicPriceCHF,
        5 => DateTimeOffset.FromUnixTimeMilliseconds(EffectiveDateUtc).UtcDateTime,
        6 => ChangeReason,
        7 => ChangedBy,
        8 => (object?)ApprovalReference,
        _ => throw new AvroRuntimeException($"Bad index {fieldPos} in Get()")
    };

    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: Gtin                        = (string)fieldValue; break;
            case 1: PreviousExFactoryPriceCHF   = Convert.ToDouble(fieldValue); break;
            case 2: NewExFactoryPriceCHF        = Convert.ToDouble(fieldValue); break;
            case 3: PreviousPublicPriceCHF      = Convert.ToDouble(fieldValue); break;
            case 4: NewPublicPriceCHF           = Convert.ToDouble(fieldValue); break;
            case 5: EffectiveDateUtc            = new DateTimeOffset((DateTime)fieldValue).ToUnixTimeMilliseconds(); break;
            case 6: ChangeReason                = (string)fieldValue; break;
            case 7: ChangedBy                   = (string)fieldValue; break;
            case 8: ApprovalReference           = fieldValue as string; break;
            default: throw new AvroRuntimeException($"Bad index {fieldPos} in Put()");
        }
    }
}
