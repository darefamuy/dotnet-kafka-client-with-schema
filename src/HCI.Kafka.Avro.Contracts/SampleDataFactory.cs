namespace HCI.Kafka.Avro.Contracts;

/// <summary>
/// Generates realistic Swiss pharmaceutical test data for training exercises.
/// All data is fictional but structurally valid per Swiss regulatory standards.
/// </summary>
public static class SampleDataFactory
{
    private static readonly Random _rng = new(42); // Seeded for reproducible tests

    // Real WHO ATC code segments (anonymised product names)
    private static readonly (string atc, string name, string substance, string form)[] Products =
    [
        ("C09AA05", "Ramipril Sandoz 5 mg", "Ramipril", "Tablette"),
        ("C09AA05", "Ramipril Mepha 10 mg", "Ramipril", "Tablette"),
        ("C09CA01", "Losartan Spirig 50 mg", "Losartan", "Filmtablette"),
        ("A10BA02", "Metformin HCL 500 mg", "Metformin", "Tablette"),
        ("A10BA02", "Metformin Mepha 850 mg", "Metformin", "Tablette"),
        ("N02BE01", "Paracetamol Streuli 500 mg", "Paracetamol", "Tablette"),
        ("J01CA04", "Amoxicillin Sandoz 500 mg", "Amoxicillin", "Kapsel"),
        ("R03AC02", "Salbutamol Inyahaler 100 mcg", "Salbutamol", "Inhalationsspray"),
        ("B01AC06", "Aspirin Cardio 100 mg", "Acetylsalicylsäure", "Tablette"),
        ("C10AA05", "Atorvastatin Spirig 20 mg", "Atorvastatin", "Filmtablette"),
        ("C10AA05", "Atorvastatin Mepha 40 mg", "Atorvastatin", "Filmtablette"),
        ("N06AB06", "Sertralin Sandoz 50 mg", "Sertralin", "Filmtablette"),
        ("N05BA01", "Diazepam Desitin 5 mg", "Diazepam", "Tablette"),
        ("J05AE03", "Ritonavir 100 mg", "Ritonavir", "Kapsel"),
        ("L01BA01", "Methotrexat Pfizer 2.5 mg", "Methotrexat", "Tablette"),
    ];

    private static readonly string[] Manufacturers =
    [
        "Sandoz Pharmaceuticals AG",
        "Mepha Pharma AG",
        "Spirig HealthCare AG",
        "Streuli Pharma AG",
        "Pfizer AG",
        "Novartis Pharma Schweiz AG",
        "Roche Pharma AG",
        "Vifor SA",
        "Galderma SA",
        "Desitin Pharma GmbH"
    ];

    private static readonly string[] DataSources =
        ["SWISSMEDIC", "BAG_SL", "COMPENDIUM", "MANUFACTURER_FEED"];

    private static readonly string[] ChangeReasons =
        ["SL_REVIEW", "THERAPEUTIC_SUBSTITUTION", "MANUFACTURER_REQUEST", "CURRENCY_ADJUSTMENT"];

    private static readonly string[] AlertHeadlines =
    [
        "Charge-Rückruf wegen Verunreinigung",
        "Lieferengpass vorübergehend",
        "Qualitätsmangel: falsche Deklaration",
        "Importverbot aufgehoben",
        "Preissistierung SL",
    ];

    /// <summary>Generates a single realistic MedicinalProduct.</summary>
    public static MedicinalProduct RandomProduct()
    {
        var (atc, name, substance, form) = Products[_rng.Next(Products.Length)];
        var exFactory = Math.Round(1.5 + _rng.NextDouble() * 200, 2);
        var publicPrice = Math.Round(exFactory * 1.295, 2); // BAG distribution margin

        return new MedicinalProduct
        {
            Gtin = GenerateGtin(),
            AuthorizationNumber = GenerateAuthNumber(),
            ProductName = name,
            AtcCode = atc,
            DosageForm = form,
            Manufacturer = Manufacturers[_rng.Next(Manufacturers.Length)],
            ActiveSubstance = substance,
            MarketingStatus = WeightedStatus(),
            ExFactoryPriceCHF = exFactory,
            PublicPriceCHF = publicPrice,
            LastUpdatedUtc = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            DataSource = DataSources[_rng.Next(DataSources.Length)],
            PackageSize = _rng.NextDouble() > 0.3 ? $"{_rng.Next(7, 120)} Tabletten" : null,
            NarcoticsCategory = _rng.NextDouble() > 0.9 ? "b" : null
        };
    }

    /// <summary>Generates a batch of products — used for load test exercises.</summary>
    public static IReadOnlyList<MedicinalProduct> GenerateBatch(int count) =>
        Enumerable.Range(0, count).Select(_ => RandomProduct()).ToList();

    /// <summary>Generates a DrugAlert for the given GTIN.</summary>
    public static DrugAlert RandomAlert(string gtin)
    {
        var types = new[] { "RECALL", "SHORTAGE", "QUALITY_DEFECT", "PRICE_SUSPENSION", "IMPORT_BAN" };
        var severities = new[] { "CLASS_I", "CLASS_II", "CLASS_III", "ADVISORY" };
        var weights = new[] { 5, 20, 40, 35 }; // CLASS_I rarest, ADVISORY most common

        return new DrugAlert
        {
            AlertId = Guid.NewGuid().ToString(),
            Gtin = gtin,
            AlertType = types[_rng.Next(types.Length)],
            Severity = WeightedSeverity(severities, weights),
            Headline = AlertHeadlines[_rng.Next(AlertHeadlines.Length)],
            Description = $"Swissmedic-Mitteilung: Dieses Produkt ist von einer Massnahme betroffen. " +
                          $"Bitte beachten Sie die offiziellen Anweisungen auf swissmedic.ch.",
            AffectedLots = GenerateLots(),
            IssuedByUtc = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ExpiresUtc = _rng.NextDouble() > 0.5
                ? DateTimeOffset.UtcNow.AddDays(30).ToUnixTimeMilliseconds()
                : null,
            SourceUrl = $"https://www.swissmedic.ch/swissmedic/de/home/humanarzneimittel/marktueberwachung/{Guid.NewGuid()}"
        };
    }

    /// <summary>Generates a PriceUpdate event — used in Lab 3D (backward compatibility).</summary>
    public static PriceUpdate RandomPriceUpdate(string gtin, bool includeApprovalRef = false)
    {
        var previous = Math.Round(10 + _rng.NextDouble() * 200, 2);
        var changePercent = (_rng.NextDouble() * 0.3) - 0.1; // -10% to +20%
        var next = Math.Round(previous * (1 + changePercent), 2);

        return new PriceUpdate
        {
            Gtin = gtin,
            PreviousExFactoryPriceCHF = previous,
            NewExFactoryPriceCHF = next,
            PreviousPublicPriceCHF = Math.Round(previous * 1.295, 2),
            NewPublicPriceCHF = Math.Round(next * 1.295, 2),
            EffectiveDateUtc = DateTimeOffset.UtcNow.AddDays(_rng.Next(1, 30)).ToUnixTimeMilliseconds(),
            ChangeReason = ChangeReasons[_rng.Next(ChangeReasons.Length)],
            ChangedBy = "BAG-SL-IMPORT",
            ApprovalReference = includeApprovalRef ? $"BAG-{_rng.Next(10000, 99999)}" : null
        };
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private static string GenerateGtin()
    {
        // 7612345 = Galenica GS1 company prefix (7 digits)
        var articleNumber = _rng.Next(100000, 999999); // 6 digits
        var baseGtin = $"7612345{articleNumber:D6}"; // 7 + 6 = 13 digits
        var checkDigit = ComputeGtinCheckDigit(baseGtin);
        return baseGtin + checkDigit; // 13 + 1 = 14 digits
    }

    private static int ComputeGtinCheckDigit(string gtin13)
    {
        var sum = 0;
        for (var i = 0; i < gtin13.Length; i++)
            sum += (gtin13[i] - '0') * (i % 2 == 0 ? 3 : 1);
        return (10 - (sum % 10)) % 10;
    }

    private static string GenerateAuthNumber() => _rng.Next(10000, 99999).ToString();

    private static string WeightedStatus()
    {
        var roll = _rng.Next(100);
        return roll switch
        {
            < 85 => "ACTIVE",
            < 92 => "PENDING",
            < 97 => "SUSPENDED",
            _    => "WITHDRAWN"
        };
    }

    private static string WeightedSeverity(string[] severities, int[] weights)
    {
        var total = weights.Sum();
        var roll = _rng.Next(total);
        var cumulative = 0;
        for (var i = 0; i < weights.Length; i++)
        {
            cumulative += weights[i];
            if (roll < cumulative) return severities[i];
        }
        return severities[^1];
    }

    private static IList<string> GenerateLots()
    {
        var count = _rng.Next(0, 5);
        return Enumerable.Range(0, count)
            .Select(_ => $"LOT{_rng.Next(100000, 999999)}")
            .ToList();
    }
}
