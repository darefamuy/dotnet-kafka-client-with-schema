using System.Text.RegularExpressions;
using FluentAssertions;
using HCI.Kafka.Avro.Contracts;
using Xunit;

namespace HCI.Kafka.Avro.Tests;

/// <summary>Tests for the SampleDataFactory — validates that generated test data
/// conforms to Swiss pharmaceutical regulatory standards.</summary>
public sealed class SampleDataFactoryTests
{
    [Fact]
    public void RandomProduct_Gtin_ShouldBe14Digits()
    {
        for (var i = 0; i < 20; i++)
        {
            var product = SampleDataFactory.RandomProduct();
            product.Gtin.Should().HaveLength(14, "GTIN must be exactly 14 digits (GS1 standard)");
            product.Gtin.Should().MatchRegex(@"^\d{14}$", "GTIN must contain only digits");
        }
    }

    [Fact]
    public void RandomProduct_Gtin_ShouldHaveValidCheckDigit()
    {
        for (var i = 0; i < 50; i++)
        {
            var product = SampleDataFactory.RandomProduct();
            var gtin = product.Gtin;
            ComputeGtinCheckDigit(gtin[..13]).ToString()
                .Should().Be(gtin[13].ToString(), $"GTIN {gtin} has invalid check digit");
        }
    }

    [Fact]
    public void RandomProduct_AtcCode_ShouldMatchWhoFormat()
    {
        // WHO ATC codes follow the format: letter(s) + digits + letter + digits + digits
        // e.g. C09AA05, N02BE01, A10BA02
        var atcPattern = new Regex(@"^[A-Z]\d{2}[A-Z]{2}\d{2}$");

        for (var i = 0; i < 30; i++)
        {
            var product = SampleDataFactory.RandomProduct();
            product.AtcCode.Should().MatchRegex(atcPattern,
                $"ATC code '{product.AtcCode}' must follow WHO format");
        }
    }

    [Fact]
    public void RandomProduct_Prices_ShouldBePositiveAndRealistic()
    {
        for (var i = 0; i < 20; i++)
        {
            var product = SampleDataFactory.RandomProduct();
            product.ExFactoryPriceCHF.Should().BeGreaterThan(0, "ex-factory price must be positive");
            product.PublicPriceCHF.Should().BeGreaterThan(product.ExFactoryPriceCHF,
                "public price must exceed ex-factory price (distribution margin)");
            product.PublicPriceCHF.Should().BeLessThan(product.ExFactoryPriceCHF * 1.5,
                "public price should not exceed ex-factory by more than 50%");
        }
    }

    [Fact]
    public void RandomProduct_MarketingStatus_ShouldBeValidEnumValue()
    {
        var validStatuses = new HashSet<string> { "ACTIVE", "SUSPENDED", "WITHDRAWN", "PENDING" };

        for (var i = 0; i < 30; i++)
        {
            var product = SampleDataFactory.RandomProduct();
            validStatuses.Should().Contain(product.MarketingStatus,
                $"'{product.MarketingStatus}' is not a valid MarketingStatus enum value");
        }
    }

    [Fact]
    public void RandomProduct_StatusDistribution_ShouldFavourActive()
    {
        // ~85% of products should be ACTIVE based on the weighted distribution
        var products = SampleDataFactory.GenerateBatch(1000);
        var activeCount = products.Count(p => p.MarketingStatus == "ACTIVE");

        activeCount.Should().BeGreaterThan(750,
            "at least 75% of generated products should be ACTIVE (realistic distribution)");
        activeCount.Should().BeLessThan(950,
            "not all products should be ACTIVE (some should have other statuses)");
    }

    [Fact]
    public void GenerateBatch_ShouldReturnRequestedCount()
    {
        var batch = SampleDataFactory.GenerateBatch(50);
        batch.Should().HaveCount(50);
    }

    [Fact]
    public void RandomAlert_ShouldHaveValidAlertType()
    {
        var validTypes = new HashSet<string>
            { "RECALL", "SHORTAGE", "QUALITY_DEFECT", "PRICE_SUSPENSION", "IMPORT_BAN" };

        for (var i = 0; i < 20; i++)
        {
            var alert = SampleDataFactory.RandomAlert("76123451234560");
            validTypes.Should().Contain(alert.AlertType);
            alert.AlertId.Should().NotBeNullOrEmpty();
            alert.Gtin.Should().Be("76123451234560");
        }
    }

    [Fact]
    public void RandomPriceUpdate_V10_ShouldHaveNullApprovalRef()
    {
        var update = SampleDataFactory.RandomPriceUpdate("76123451234560", includeApprovalRef: false);
        update.ApprovalReference.Should().BeNull();
    }

    [Fact]
    public void RandomPriceUpdate_V11_ShouldHaveNonNullApprovalRef()
    {
        var update = SampleDataFactory.RandomPriceUpdate("76123451234560", includeApprovalRef: true);
        update.ApprovalReference.Should().NotBeNullOrEmpty();
        update.ApprovalReference.Should().StartWith("BAG-");
    }

    private static int ComputeGtinCheckDigit(string gtin13)
    {
        var sum = 0;
        for (var i = 0; i < gtin13.Length; i++)
            sum += (gtin13[i] - '0') * (i % 2 == 0 ? 3 : 1);
        return (10 - (sum % 10)) % 10;
    }
}
