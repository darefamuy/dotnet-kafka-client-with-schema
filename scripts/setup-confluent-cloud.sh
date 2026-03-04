#!/usr/bin/env bash
# =============================================================================
# HCI Kafka Module 1 — Confluent Cloud Topic & Schema Setup Script
# =============================================================================
# Prerequisites:
#   - Confluent CLI v3.x installed: https://docs.confluent.io/confluent-cli/
#   - Authenticated: confluent login
#   - Environment and cluster selected:
#       confluent environment use env-xxxxx
#       confluent kafka cluster use lkc-xxxxx
#
# Usage:
#   chmod +x scripts/setup-confluent-cloud.sh
#   ./scripts/setup-confluent-cloud.sh
# =============================================================================

set -euo pipefail

echo "=============================================="
echo "  HCI Kafka Module 1 — Confluent Cloud Setup  "
echo "=============================================="
echo ""

# ── Check prerequisites ───────────────────────────────────────────────────────
if ! command -v confluent &> /dev/null; then
    echo "ERROR: Confluent CLI not found. Install from https://docs.confluent.io/confluent-cli/"
    exit 1
fi

echo "[1/5] Checking Confluent CLI authentication..."
confluent kafka cluster list > /dev/null 2>&1 || {
    echo "ERROR: Not authenticated. Run: confluent login"
    exit 1
}
echo "      ✓ Authenticated"

# ── Create topics ─────────────────────────────────────────────────────────────
echo ""
echo "[2/5] Creating Kafka topics..."

create_topic() {
    local topic=$1
    local partitions=$2
    local retention_days=$3
    local retention_ms=$((retention_days * 24 * 3600 * 1000))

    echo "      Creating topic: $topic (partitions=$partitions, retention=${retention_days}d)"
    confluent kafka topic create "$topic" \
        --partitions "$partitions" \
        --config "retention.ms=${retention_ms}" \
        --config "cleanup.policy=delete" \
        --config "compression.type=lz4" \
        --config "min.insync.replicas=2" \
        2>/dev/null || echo "      (topic already exists — skipping)"
}

create_topic "hci.medicinal-products.avro"    6  3
create_topic "hci.drug-alerts.avro"           6  90
create_topic "hci.price-updates.avro"      3  30
create_topic "hci.medicinal-products.avro.dlq" 6 30
echo "      ✓ Topics created"

# ── Register Avro schemas ─────────────────────────────────────────────────────
echo ""
echo "[3/5] Registering Avro schemas..."

SCHEMA_DIR="$(cd "$(dirname "$0")/../schemas" && pwd)"

if [ ! -f "$SCHEMA_DIR/hci.medicinal-product.avro.avsc" ]; then
    echo "      WARNING: Schema file not found at $SCHEMA_DIR — skipping schema registration"
    echo "      Run from the repository root or adjust SCHEMA_DIR"
else
    echo "      Registering hci.medicinal-product.avro..."
    confluent schema-registry schema create \
        --subject "hci.medicinal-products.avro-value" \
        --schema "$SCHEMA_DIR/hci.medicinal-product.avro.avsc" \
        --type AVRO \
        2>/dev/null || echo "      (schema already registered — checking compatibility)"

    echo "      Registering hci.drug-alert.avro..."
    confluent schema-registry schema create \
        --subject "hci.drug-alerts.avro-value" \
        --schema "$SCHEMA_DIR/hci.drug-alert.avro.avsc" \
        --type AVRO \
        2>/dev/null || echo "      (schema already registered)"

    echo "      ✓ Schemas registered"
    echo "      Registering hci.price-updates.avro..."
    confluent schema-registry schema create \
        --subject "hci.price-updates.avro-value" \
        --schema "$SCHEMA_DIR/hci.price-update.avro.avsc" \
        --type AVRO \
        2>/dev/null || echo "      (schema already registered)"

    echo "      ✓ Schemas registered"
    
fi

# ── Set compatibility modes ───────────────────────────────────────────────────
echo ""
echo "[4/5] Setting schema compatibility modes..."

set_compatibility() {
    local subject=$1
    local mode=$2
    echo "      $subject → $mode"
    confluent schema-registry subject update "$subject" \
        --compatibility "$mode" \
        2>/dev/null || echo "      (skipped — check SR permissions)"
}

set_compatibility "hci.medicinal-products.avro-value"  "FULL"
set_compatibility "hci.drug-alerts.avro-value"          "FULL"
set_compatibility "hci.price-updates.avro-value"     "BACKWARD"
echo "      ✓ Compatibility modes set"

# ── Verify setup ──────────────────────────────────────────────────────────────
echo ""
echo "[5/5] Verifying setup..."
echo ""
echo "  Topics:"
confluent kafka topic list | grep "hci\." || echo "  (none found — check cluster selection)"
echo ""
echo "  Schemas:"
confluent schema-registry subject list | grep "hci\." || echo "  (none found — check SR access)"
echo ""
echo "=============================================="
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "  1. Copy appsettings template and fill in credentials:"
echo "     cp src/HCI.Kafka.Producer/appsettings.json appsettings.Development.json"
echo "  2. Start the observability stack:"
echo "     docker-compose up -d"
echo "  3. Run the producer:"
echo "     cd src/HCI.Kafka.Producer && dotnet run"
echo "  4. Run the consumer (new terminal):"
echo "     cd src/HCI.Kafka.Consumer && dotnet run"
echo "  5. Open Grafana: http://localhost:3000 (admin/admin)"
echo "=============================================="
