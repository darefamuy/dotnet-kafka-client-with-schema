using Avro;
using Avro.IO;
using Avro.Specific;
using FluentAssertions;
using HCI.Kafka.Avro.Contracts;
using Xunit;

namespace HCI.Kafka.Avro.Tests;

/// <summary>
/// Avro round-trip tests — verifies that serialising and deserialising a record
/// produces an identical object without any data loss or type coercion errors.
///
/// These tests use the Avro binary encoding directly (without Kafka or Schema Registry)
/// to validate the SpecificRecord implementations in isolation.
///
/// WHY TEST WITHOUT KAFKA?
/// ─────────────────────────────────────────────────────────────────────────────
/// Unit tests should not depend on external infrastructure. These tests validate:
///   1. All fields are correctly mapped in Get()/Put()
///   2. Nullable fields (union types) serialise/deserialise as null correctly
///   3. Nested types (enums, arrays, timestamps) work correctly
///   4. The Schema JSON string is valid Avro and parseable
///
/// Integration tests (with real Confluent Cloud) are in scripts/integration-test.sh
/// </summary>
public sealed class AvroRoundTripTests
{
    // ── MedicinalProduct ──────────────────────────────────────────────────────

    [Fact]
    public void MedicinalProduct_RoundTrip_ShouldPreserveAllFields()
    {
        var original = new MedicinalProduct
        {
            Gtin                = "76123451234560",
            AuthorizationNumber = "68177",
            ProductName         = "Ramipril Sandoz 5 mg",
            AtcCode             = "C09AA05",
            DosageForm          = "Tablette",
            Manufacturer        = "Sandoz Pharmaceuticals AG",
            ActiveSubstance     = "Ramipril",
            MarketingStatus     = "ACTIVE",
            ExFactoryPriceCHF   = 12.50,
            PublicPriceCHF      = 16.19,
            LastUpdatedUtc      = 1700000000000L,
            DataSource          = "SWISSMEDIC",
            PackageSize         = "28 Tabletten",
            NarcoticsCategory   = null
        };

        var deserialized = RoundTrip(original);

        deserialized.Gtin.Should().Be(original.Gtin);
        deserialized.AuthorizationNumber.Should().Be(original.AuthorizationNumber);
        deserialized.ProductName.Should().Be(original.ProductName);
        deserialized.AtcCode.Should().Be(original.AtcCode);
        deserialized.DosageForm.Should().Be(original.DosageForm);
        deserialized.Manufacturer.Should().Be(original.Manufacturer);
        deserialized.ActiveSubstance.Should().Be(original.ActiveSubstance);
        deserialized.MarketingStatus.Should().Be(original.MarketingStatus);
        deserialized.ExFactoryPriceCHF.Should().BeApproximately(original.ExFactoryPriceCHF, 0.001);
        deserialized.PublicPriceCHF.Should().BeApproximately(original.PublicPriceCHF, 0.001);
        deserialized.LastUpdatedUtc.Should().Be(original.LastUpdatedUtc);
        deserialized.DataSource.Should().Be(original.DataSource);
        deserialized.PackageSize.Should().Be(original.PackageSize);
        deserialized.NarcoticsCategory.Should().BeNull();
    }

    [Fact]
    public void MedicinalProduct_WithNullOptionalFields_RoundTrip_ShouldPreserveNulls()
    {
        var original = new MedicinalProduct
        {
            Gtin                = "76123459999990",
            AuthorizationNumber = "12345",
            ProductName         = "Test Product",
            AtcCode             = "N02BE01",
            DosageForm          = "Tablette",
            Manufacturer        = "Test Pharma AG",
            ActiveSubstance     = "Paracetamol",
            MarketingStatus     = "SUSPENDED",
            ExFactoryPriceCHF   = 5.00,
            PublicPriceCHF      = 6.48,
            LastUpdatedUtc      = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            DataSource          = "BAG_SL",
            PackageSize         = null,       // optional — null
            NarcoticsCategory   = null        // optional — null
        };

        var deserialized = RoundTrip(original);

        deserialized.PackageSize.Should().BeNull("null optional field must survive round-trip");
        deserialized.NarcoticsCategory.Should().BeNull("null optional field must survive round-trip");
        deserialized.MarketingStatus.Should().Be("SUSPENDED");
    }

    [Fact]
    public void MedicinalProduct_WithNarcoticsCategory_RoundTrip_ShouldPreserveValue()
    {
        var original = SampleDataFactory.RandomProduct();
        original.NarcoticsCategory = "b";

        var deserialized = RoundTrip(original);

        deserialized.NarcoticsCategory.Should().Be("b");
    }

    [Theory]
    [InlineData("ACTIVE")]
    [InlineData("SUSPENDED")]
    [InlineData("WITHDRAWN")]
    [InlineData("PENDING")]
    public void MedicinalProduct_AllMarketingStatuses_RoundTrip(string status)
    {
        var product = SampleDataFactory.RandomProduct();
        product.MarketingStatus = status;

        var deserialized = RoundTrip(product);

        deserialized.MarketingStatus.Should().Be(status);
    }

    [Fact]
    public void MedicinalProduct_BatchOf100_AllRoundTripSuccessfully()
    {
        var products = SampleDataFactory.GenerateBatch(100);
        var failures = new List<string>();

        foreach (var product in products)
        {
            try
            {
                var deserialized = RoundTrip(product);
                if (deserialized.Gtin != product.Gtin)
                    failures.Add($"GTIN mismatch: {product.Gtin}");
            }
            catch (Exception ex)
            {
                failures.Add($"Exception for GTIN {product.Gtin}: {ex.Message}");
            }
        }

        failures.Should().BeEmpty("all 100 products must round-trip without errors");
    }

    // ── DrugAlert ────────────────────────────────────────────────────────────

    [Fact]
    public void DrugAlert_WithAffectedLots_RoundTrip_ShouldPreserveLotList()
    {
        var original = new DrugAlert
        {
            AlertId     = Guid.NewGuid().ToString(),
            Gtin        = "76123451234560",
            AlertType   = "RECALL",
            Severity    = "CLASS_I",
            Headline    = "Charge-Rückruf wegen Verunreinigung",
            Description = "Swissmedic-Mitteilung: Produkt zurückrufen.",
            AffectedLots = new List<string> { "LOT123456", "LOT789012", "LOT345678" },
            IssuedByUtc = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ExpiresUtc  = null,
            SourceUrl   = "https://www.swissmedic.ch/test"
        };

        var deserialized = RoundTrip(original);

        deserialized.AlertId.Should().Be(original.AlertId);
        deserialized.AlertType.Should().Be("RECALL");
        deserialized.Severity.Should().Be("CLASS_I");
        deserialized.AffectedLots.Should().HaveCount(3);
        deserialized.AffectedLots.Should().BeEquivalentTo(original.AffectedLots);
        deserialized.ExpiresUtc.Should().BeNull();
    }

    [Fact]
    public void DrugAlert_WithEmptyAffectedLots_RoundTrip_ShouldPreserveEmptyList()
    {
        var original = new DrugAlert
        {
            AlertId      = Guid.NewGuid().ToString(),
            Gtin         = "76123451234560",
            AlertType    = "SHORTAGE",
            Severity     = "ADVISORY",
            Headline     = "Lieferengpass",
            Description  = "Temporärer Lieferengpass",
            AffectedLots = new List<string>(), // empty list
            IssuedByUtc  = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ExpiresUtc   = DateTimeOffset.UtcNow.AddDays(30).ToUnixTimeMilliseconds(),
            SourceUrl    = "https://www.bag.admin.ch/test"
        };

        var deserialized = RoundTrip(original);

        deserialized.AffectedLots.Should().BeEmpty("empty array should survive round-trip");
        deserialized.ExpiresUtc.Should().NotBeNull("non-null ExpiresUtc should survive round-trip");
    }

    [Theory]
    [InlineData("RECALL")]
    [InlineData("SHORTAGE")]
    [InlineData("QUALITY_DEFECT")]
    [InlineData("PRICE_SUSPENSION")]
    [InlineData("IMPORT_BAN")]
    public void DrugAlert_AllAlertTypes_RoundTrip(string alertType)
    {
        var alert = SampleDataFactory.RandomAlert("76123451234560");
        alert.AlertType = alertType;

        var deserialized = RoundTrip(alert);

        deserialized.AlertType.Should().Be(alertType);
    }

    // ── PriceUpdate ──────────────────────────────────────────────────────────

    [Fact]
    public void PriceUpdate_V1_WithNullApprovalRef_RoundTrip()
    {
        var original = SampleDataFactory.RandomPriceUpdate("76123451234560", includeApprovalRef: false);

        var deserialized = RoundTrip(original);

        deserialized.Gtin.Should().Be(original.Gtin);
        deserialized.ApprovalReference.Should().BeNull("v1.0 event has no approval reference");
    }

    [Fact]
    public void PriceUpdate_V11_WithApprovalRef_RoundTrip()
    {
        var original = SampleDataFactory.RandomPriceUpdate("76123451234560", includeApprovalRef: true);
        original.ApprovalReference = "BAG-99999";

        var deserialized = RoundTrip(original);

        deserialized.ApprovalReference.Should().Be("BAG-99999");
    }

    [Fact]
    public void PriceUpdate_PriceCalculations_ShouldPreservePrecision()
    {
        var original = new PriceUpdate
        {
            Gtin                      = "76123451234560",
            PreviousExFactoryPriceCHF = 123.45,
            NewExFactoryPriceCHF      = 118.90,
            PreviousPublicPriceCHF    = 159.87,
            NewPublicPriceCHF         = 153.98,
            EffectiveDateUtc          = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ChangeReason              = "SL_REVIEW",
            ChangedBy                 = "BAG-SL-IMPORT",
            ApprovalReference         = null
        };

        var deserialized = RoundTrip(original);

        deserialized.PreviousExFactoryPriceCHF.Should().BeApproximately(123.45, 0.001);
        deserialized.NewExFactoryPriceCHF.Should().BeApproximately(118.90, 0.001);
        deserialized.PreviousPublicPriceCHF.Should().BeApproximately(159.87, 0.001);
        deserialized.NewPublicPriceCHF.Should().BeApproximately(153.98, 0.001);
    }

    // ── Schema validation ─────────────────────────────────────────────────────

    [Fact]
    public void MedicinalProductSchema_ShouldBeValidAvro()
    {
        var act = () => Schema.Parse(SchemaDefinitions.MedicinalProductSchemaJson);
        act.Should().NotThrow("schema JSON must be valid Avro");
    }

    [Fact]
    public void DrugAlertSchema_ShouldBeValidAvro()
    {
        var act = () => Schema.Parse(SchemaDefinitions.DrugAlertSchemaJson);
        act.Should().NotThrow();
    }

    [Fact]
    public void PriceUpdateSchema_ShouldBeValidAvro()
    {
        var act = () => Schema.Parse(SchemaDefinitions.PriceUpdateSchemaJson);
        act.Should().NotThrow();
    }

    [Fact]
    public void MedicinalProductSchema_ShouldHave14Fields()
    {
        var schema = (RecordSchema)Schema.Parse(SchemaDefinitions.MedicinalProductSchemaJson);
        schema.Fields.Should().HaveCount(14, "MedicinalProduct v1 schema has exactly 14 fields");
    }

    [Fact]
    public void DrugAlertSchema_ShouldHaveCorrectNamespace()
    {
        var schema = (RecordSchema)Schema.Parse(SchemaDefinitions.DrugAlertSchemaJson);
        schema.Namespace.Should().Be("hci");
        schema.Name.Should().Be("DrugAlert");
    }

    [Fact]
    public void PriceUpdateSchema_ApprovalReferenceField_ShouldBeNullable()
    {
        var schema = (RecordSchema)Schema.Parse(SchemaDefinitions.PriceUpdateSchemaJson);
        var approvalField = schema.Fields.FirstOrDefault(f => f.Name == "approvalReference");

        approvalField.Should().NotBeNull("approvalReference field must exist");
        approvalField!.Schema.Tag.Should().Be(Schema.Type.Union,
            "approvalReference must be a union type (nullable)");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Serialises a SpecificRecord to bytes using the Avro DatumWriter,
    /// then deserialises using the DatumReader. This is exactly what the
    /// AvroSerializer/AvroDeserializer do internally (minus the wire format header).
    /// </summary>
    private static T RoundTrip<T>(T record) where T : ISpecificRecord, new()
    {
        var schema = record.Schema;

        // Serialise
        var writer = new SpecificDefaultWriter(schema);
        using var ms = new MemoryStream();
        var encoder = new BinaryEncoder(ms);
        writer.Write(record, encoder);
        encoder.Flush();
        var bytes = ms.ToArray();

        bytes.Should().NotBeEmpty("serialised bytes must not be empty");

        // Deserialise
        var reader = new SpecificDefaultReader(schema, schema);
        using var readMs = new MemoryStream(bytes);
        var decoder = new BinaryDecoder(readMs);
        var result = new T();
        reader.Read(result, decoder);

        return result;
    }
}
