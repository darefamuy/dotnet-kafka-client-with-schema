# Pharma Confluent Cloud Kafka Training

## Overview

This project uses **Apache Avro** backed by **Confluent Schema Registry**. You will learn how Avro's binary encoding works, how Schema Registry enforces compatibility rules across teams, and how to evolve schemas safely without breaking existing producers or consumers.

Without schema governance, a producer-side change (renaming a field, removing a field, changing a type) can silently break any or all of these consumers. 
For instance, a Schema Registry with FULL compatibility makes breaking changes **impossible to deploy accidentally**.

---

## Project Structure

```
hci-kafka-module3/
├── HCI.Kafka.Module3.sln
│
├── src/
│   ├── HCI.Kafka.Avro.Contracts/              # Shared domain contracts library
│   │   ├── SpecificRecords.cs                 # MedicinalProduct, DrugAlert, PriceUpdate
│   │   │                                      # (ISpecificRecord implementations)
│   │   ├── SchemaDefinitions.cs               # Canonical Avro schema JSON strings
│   │   └── SampleDataFactory.cs               # Swiss pharma test data generator
│   │
│   ├── HCI.Kafka.Avro.SchemaRegistry/         # Schema Registry management
│   │   ├── Configuration/
│   │   │   └── Options.cs                     # KafkaOptions + SchemaRegistryOptions
│   │   ├── HciSchemaRegistrar.cs              # Register, validate, inspect schemas
│   │   └── SchemaRegistryClientFactory.cs     # Build CachedSchemaRegistryClient
│   │
│   ├── HCI.Kafka.Avro.Producer/               # Avro producer application
│   │   ├── AvroMedicinalProductProducer.cs    # Full Avro producer with AutoRegister
│   │   ├── AvroDrugAlertProducer.cs           # Alert producer (AutoRegister=false)
│   │   ├── AvroPriceUpdateProducer.cs         # Price producer (backward compat demo)
│   │   ├── ProducerWorker.cs                  # Orchestrates the full demo sequence
│   │   └── Program.cs                         # DI + startup
│   │
│   └── HCI.Kafka.Avro.Consumer/               # Avro consumer application
│       ├── AvroMedicinalProductConsumer.cs    # Consumer with schema resolution notes
│       ├── AvroDrugAlertConsumer.cs           # Header-first routing + full deserialization
│       ├── AvroPriceUpdateConsumer.cs         # Reads v1.0 and v1.1 events transparently
│       └── Program.cs                         # Runs all three consumers as BackgroundServices
│
├── tests/
│   └── HCI.Kafka.Avro.Tests/
│       ├── AvroRoundTripTests.cs              # Serialise→deserialise without Kafka/SR
│       ├── SchemaCompatibilityTests.cs        # Avro schema resolution correctness
│       └── SampleDataFactoryTests.cs          # Validates generated pharma data
│
├── schemas/                                   # Canonical .avsc files (source of truth)
│   ├── hci.medicinal-product.avro.avsc
│   ├── hci.drug-alert.avro.avsc
│   └── hci.price-update.avro.avsc
│
└── scripts/
    └── setup-confluent-cloud.sh                      # Sets up topics and schemas
```

---

## Quick Start

### Step 1 — Prerequisites

- Topics exist in Confluent Cloud. You can run ./scripts/setup-confluent-cloud.sh to setup the topics and schemas.
- Schema Registry is enabled on your Confluent Cloud cluster
- .NET 10 SDK installed

### Step 2 — Get Schema Registry credentials

In Confluent Cloud:
1. **Environments** → your cluster → **Schema Registry**
2. Click **Add key** under **Credentials**
3. Note the **Endpoint URL** (e.g. `https://psrc-xxxxx.westeurope.azure.confluent.cloud`)
4. Note the **API Key** and **API Secret**

### Step 3 — Configure credentials

```bash
# Copy and fill in credentials for the producer
cp src/HCI.Kafka.Avro.Producer/appsettings.json \
   src/HCI.Kafka.Avro.Producer/appsettings.Development.json

# Copy and fill in credentials for the consumer
cp src/HCI.Kafka.Avro.Consumer/appsettings.json \
   src/HCI.Kafka.Avro.Consumer/appsettings.Development.json
```

Edit both `appsettings.Development.json` files:
```json
{
  "Kafka": {
    "BootstrapServers": "pkc-XXXXX.westeurope.azure.confluent.cloud:9092",
    "ApiKey":    "YOUR_KAFKA_API_KEY",
    "ApiSecret": "YOUR_KAFKA_API_SECRET"
  },
  "SchemaRegistry": {
    "Url":       "https://psrc-XXXXX.westeurope.azure.confluent.cloud",
    "ApiKey":    "YOUR_SR_API_KEY",
    "ApiSecret": "YOUR_SR_API_SECRET"
  }
}
```

### Step 4 — Build the solution

```bash
dotnet build
```

### Step 5 — Run the tests (no credentials needed)

```bash
dotnet test --logger "console;verbosity=normal"
```

All tests are unit tests that run without Kafka or Schema Registry. Expected: **all green**.

### Step 6 — Run the producer

```bash
cd src/HCI.Kafka.Avro.Producer
DOTNET_ENVIRONMENT=Development dotnet run
```

The producer:
1. Produces 100 MedicinalProduct Avro messages
2. Produces 5 DrugAlert messages
3. Produces 20 PriceUpdate messages (mix of v1.0 and v1.1)

Expected output:
```
[10:30:02.567 INF] ✓ Produced 100 MedicinalProduct Avro messages in 843ms (118 msg/s)
[10:30:02.891 INF] ✓ Produced 5 DrugAlert Avro messages
[10:30:03.012 INF] ✓ Produced PriceUpdate events (v1 and v1.1 formats)
```

### Step 7 — Verify schemas in Confluent Cloud UI

1. Go to **Confluent Cloud → Schema Registry → Subjects**
2. You should see three subjects:
   - `hci.medicinal-products.avro-value`
   - `hci.drug-alerts.avro-value`
   - `hci.price-updates.avro-value`
3. Click each subject to see the schema JSON, version history, and compatibility mode

### Step 8 — Run the consumer

```bash
cd src/HCI.Kafka.Avro.Consumer
DOTNET_ENVIRONMENT=Development dotnet run
```

Expected output showing Avro-deserialised pharmaceutical data:
```
[10:30:10.123 INF] ✓ Consumed: GTIN=76123457654321 [C09AA05] Ramipril Sandoz 5 mg CHF 16.19
[10:30:10.234 WRN] ⚠  Status change: GTIN=76123459876540 Product=Metformin Mepha Status=SUSPENDED
[10:30:10.345 INF] 🚨 CLASS_I ALERT: alertId=abc123 type=RECALL gtin=76123457654321
[10:30:10.456 INF] 💰 PriceUpdate (schema v1.1): GTIN=76123451234560 16.19 → 18.50 CHF (▲14.3%)
```

---

## Core Concepts Deep-Dive

### The Confluent Wire Format

Every Avro message produced by `AvroSerializer` has a 5-byte prefix:

```
Offset  Size  Content
──────  ────  ──────────────────────────────────────────────────
0       1     Magic byte = 0x00 (identifies Confluent SR format)
1–4     4     Schema ID (big-endian int32, e.g. 0x00000001 = ID 1)
5+      N     Avro binary-encoded payload
```

```
┌──────┬──────────────────┬──────────────────────────────────────────────┐
│ 0x00 │ 0x00 0x00 0x00 1 │  Avro binary: gtin + authorizationNumber...  │
└──────┴──────────────────┴──────────────────────────────────────────────┘
  Magic    Schema ID = 1            Avro payload
```

This is why `AvroDeserializer` requires a `SchemaRegistryClient` — it reads the 4-byte schema ID and fetches the schema to decode the payload.

### CachedSchemaRegistryClient — the performance key

```
First message:                      Subsequent messages:
─────────────                       ────────────────────
Produce call                        Produce call
    │                                   │
    ▼                                   ▼
AvroSerializer checks cache         AvroSerializer checks cache
    │ MISS                              │ HIT
    ▼                                   ▼
HTTP GET /subjects/.../versions     Returns schema ID immediately
    │                                   │
    ▼                                   ▼
Cache: fingerprint → schema ID      Encode payload (zero SR calls)
    │
    ▼
Encode payload
```

At 10,000 msg/s with caching: ~0 Schema Registry calls.  
Without caching: 10,000 HTTP requests/second to Schema Registry.


## Further Reading

- [Confluent Schema Registry docs](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Avro specification](https://avro.apache.org/docs/current/spec.html)
- [Schema compatibility types explained](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
- [Confluent .NET Avro serialiser docs](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#avro)
- [Swissmedic drug authorisation database](https://www.swissmedic.ch)

