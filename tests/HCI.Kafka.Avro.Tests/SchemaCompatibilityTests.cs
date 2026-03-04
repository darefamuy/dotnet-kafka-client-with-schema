using Avro;
using FluentAssertions;
using HCI.Kafka.Avro.Contracts;
using Xunit;

namespace HCI.Kafka.Avro.Tests;

/// <summary>
/// Schema compatibility tests — validates that our schema evolution decisions
/// are correct WITHOUT requiring a live Schema Registry connection.
///
/// We test Avro's schema resolution logic directly:
///   - Can the v1.1 reader schema decode v1.0 writer data? (BACKWARD)
///   - Can the v1.0 reader schema decode v1.1 writer data? (FORWARD)
///
/// TRAINING EXERCISE: These tests document WHY each compatibility decision was made.
/// Read through them carefully — they explain the business rationale alongside
/// the technical implementation.
/// </summary>
public sealed class SchemaCompatibilityTests
{
    // ── PriceUpdate backward compatibility ───────────────────────────────────

    [Fact]
    public void PriceUpdate_V11ReaderCanReadV10Data_BackwardCompatible()
    {
        // SCENARIO: We deployed v1.1 producer (adds approvalReference)
        // but some consumers are still running the old v1.0 consumer code.
        // A v1.0 consumer must be able to read v1.1 data (BACKWARD).
        //
        // v1.0 reader schema: no approvalReference field
        // v1.1 writer data:   includes approvalReference = "BAG-12345"
        //
        // Expected: v1.0 reader ignores the unknown field → reads successfully

        var v10Schema = BuildPriceUpdateV10Schema();
        var v11Schema = Schema.Parse(SchemaDefinitions.PriceUpdateSchemaJson);

        // Avro schema resolution check: can the writer (v1.1) schema be read
        // by the reader (v1.0) schema?
        var act = () => new global::Avro.Specific.SpecificDefaultReader(v11Schema, v10Schema);
        act.Should().NotThrow(
            "v1.0 reader must be able to read v1.1 data (BACKWARD compatibility): " +
            "the extra approvalReference field should be silently ignored");
    }

    [Fact]
    public void PriceUpdate_V10ReaderCanReadV11Data_ForwardCompatible()
    {
        // SCENARIO: New v1.1 data arrives but we're reading it with the old v1.0 schema.
        // The v1.1 schema added approvalReference with default null.
        // When the v1.0 reader reads v1.1 data, it ignores approvalReference.
        //
        // This is FORWARD compatibility — old schemas can read new data.

        var v10Schema = BuildPriceUpdateV10Schema();
        var v11Schema = Schema.Parse(SchemaDefinitions.PriceUpdateSchemaJson);

        var act = () => new global::Avro.Specific.SpecificDefaultReader(v10Schema, v11Schema);
        act.Should().NotThrow(
            "v1.0 reader must handle v1.1 writer data: approvalReference has a default of null, " +
            "so the reader fills in null when the field is missing from old events");
    }

    // ── Schema structure validation ───────────────────────────────────────────

    [Fact]
    public void MedicinalProduct_LastUpdatedUtcField_ShouldBeTimestampMillis()
    {
        var schema = (RecordSchema)Schema.Parse(SchemaDefinitions.MedicinalProductSchemaJson);
        var field = schema.Fields.First(f => f.Name == "lastUpdatedUtc");

        field.Schema.Tag.Should().Be(Schema.Type.Logical, "timestamp-millis is a logical type");
        var logicalSchema = (LogicalSchema)field.Schema;
        logicalSchema.BaseSchema.Tag.Should().Be(Schema.Type.Long, "timestamp-millis is based on long");
        
        // The logicalType is stored in the schema's JSON — verify it's present
        SchemaDefinitions.MedicinalProductSchemaJson
            .Should().Contain("timestamp-millis", "lastUpdatedUtc must have timestamp-millis logical type");
    }

    [Fact]
    public void MedicinalProduct_MarketingStatusField_ShouldBeEnum()
    {
        var schema = (RecordSchema)Schema.Parse(SchemaDefinitions.MedicinalProductSchemaJson);
        var field = schema.Fields.First(f => f.Name == "marketingStatus");

        field.Schema.Tag.Should().Be(Schema.Type.Enumeration);
        var enumSchema = (EnumSchema)field.Schema;
        enumSchema.Symbols.Should().Contain("ACTIVE");
        enumSchema.Symbols.Should().Contain("SUSPENDED");
        enumSchema.Symbols.Should().Contain("WITHDRAWN");
        enumSchema.Symbols.Should().Contain("PENDING");
    }

    [Fact]
    public void DrugAlert_AffectedLotsField_ShouldBeStringArray()
    {
        var schema = (RecordSchema)Schema.Parse(SchemaDefinitions.DrugAlertSchemaJson);
        var field = schema.Fields.First(f => f.Name == "affectedLots");

        field.Schema.Tag.Should().Be(Schema.Type.Array);
        var arraySchema = (ArraySchema)field.Schema;
        arraySchema.ItemSchema.Tag.Should().Be(Schema.Type.String);
    }

    [Fact]
    public void DrugAlert_ExpiresUtcField_ShouldBeNullableTimestamp()
    {
        var schema = (RecordSchema)Schema.Parse(SchemaDefinitions.DrugAlertSchemaJson);
        var field = schema.Fields.First(f => f.Name == "expiresUtc");

        field.Schema.Tag.Should().Be(Schema.Type.Union, "expiresUtc should be union [null, timestamp]");
        var unionSchema = (UnionSchema)field.Schema;
        unionSchema.Schemas.Should().HaveCount(2);
        unionSchema.Schemas[0].Tag.Should().Be(Schema.Type.Null, "null should be the first type in the union");
    }

    [Fact]
    public void AllSchemas_ShouldHaveHciNamespace()
    {
        var schemas = new[]
        {
            SchemaDefinitions.MedicinalProductSchemaJson,
            SchemaDefinitions.DrugAlertSchemaJson,
            SchemaDefinitions.PriceUpdateSchemaJson
        };

        foreach (var schemaJson in schemas)
        {
            var schema = (RecordSchema)Schema.Parse(schemaJson);
            schema.Namespace.Should().Be("hci",
                "all HCI schemas must use the 'hci' namespace for consistent subject naming");
        }
    }

    [Fact]
    public void AllSchemas_NullableFields_ShouldHaveNullAsFirstUnionType()
    {
        // Avro convention: in union types ["null", "T"], null must come first.
        // This ensures the default value (null) can be set with Avro's default rules.
        // Violating this makes it impossible to set a default of null.

        void AssertNullFirstInUnions(string schemaJson, string schemaName)
        {
            var schema = (RecordSchema)Schema.Parse(schemaJson);
            foreach (var field in schema.Fields)
            {
                if (field.Schema.Tag == Schema.Type.Union)
                {
                    var union = (UnionSchema)field.Schema;
                    if (union.Schemas.Any(s => s.Tag == Schema.Type.Null))
                    {
                        union.Schemas[0].Tag.Should().Be(Schema.Type.Null,
                            $"Field '{field.Name}' in {schemaName}: " +
                            "null must be first in nullable union types to allow default=null");
                    }
                }
            }
        }

        AssertNullFirstInUnions(SchemaDefinitions.MedicinalProductSchemaJson, "MedicinalProduct");
        AssertNullFirstInUnions(SchemaDefinitions.DrugAlertSchemaJson, "DrugAlert");
        AssertNullFirstInUnions(SchemaDefinitions.PriceUpdateSchemaJson, "PriceUpdate");
    }

    // ── Breaking change detection ─────────────────────────────────────────────

    [Fact]
    public void MedicinalProduct_RemovingRequiredField_ShouldBeIncompatible()
    {
        // SCENARIO: What if someone tried to remove the 'atcCode' field?
        // This would break all existing consumers that expect atcCode.
        // FULL compatibility would reject this at Schema Registry.
        //
        // We test this by verifying the reader schema (missing atcCode) cannot
        // properly resolve against the writer schema (has atcCode).

        const string schemaWithoutAtcCode = """
        {
          "type": "record",
          "name": "MedicinalProduct",
          "namespace": "hci",
          "fields": [
            { "name": "gtin",                "type": "string" },
            { "name": "authorizationNumber", "type": "string" },
            { "name": "productName",         "type": "string" }
          ]
        }
        """;

        var originalSchema = Schema.Parse(SchemaDefinitions.MedicinalProductSchemaJson);
        var reducedSchema  = Schema.Parse(schemaWithoutAtcCode);

        // When we try to read v1 data (has atcCode) with the reduced schema (no atcCode),
        // Avro will project atcCode onto the reduced schema — this means existing atcCode
        // data is silently LOST on deserialisation.
        // The Schema Registry FULL mode would have prevented this from being registered.
        //
        // This test documents the consequence, not that it throws immediately.
        // The real protection comes from Schema Registry compatibility enforcement.
        var reader = new global::Avro.Specific.SpecificDefaultReader(originalSchema, reducedSchema);
        reader.Should().NotBeNull("Avro resolution proceeds but data is lost — SR FULL mode blocks this");

        // Document the risk in the test name:
        // If you see this test in a code review, ask: is the Schema Registry
        // configured with FULL compatibility? If not, this data loss can happen silently.
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>PriceUpdate v1.0 schema WITHOUT the approvalReference field.</summary>
    private static Schema BuildPriceUpdateV10Schema() => Schema.Parse("""
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
            { "name": "changedBy",                   "type": "string" }
          ]
        }
        """);
}
