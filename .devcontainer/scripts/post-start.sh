#!/bin/bash
# post-start.sh - Run every time the container starts
set -e

echo "=== Snuba devcontainer post-start checks ==="

# Initialize firewall (optional - will fail gracefully if not supported)
if [ -x /usr/local/bin/init-firewall.sh ]; then
    echo "Initializing firewall..."
    sudo /usr/local/bin/init-firewall.sh || echo "Warning: Firewall initialization failed (may not be supported in this environment)"
fi

cd /workspace

# Activate venv
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# Wait for ClickHouse
echo "Waiting for ClickHouse..."
timeout=60
counter=0
until curl -s http://clickhouse:8123/ping > /dev/null 2>&1; do
    counter=$((counter + 1))
    if [ $counter -ge $timeout ]; then
        echo "Error: ClickHouse did not become ready in time"
        exit 1
    fi
    sleep 1
done
echo "ClickHouse is ready"

# Wait for Kafka
echo "Waiting for Kafka..."
counter=0
until nc -z kafka 9092 > /dev/null 2>&1; do
    counter=$((counter + 1))
    if [ $counter -ge $timeout ]; then
        echo "Error: Kafka did not become ready in time"
        exit 1
    fi
    sleep 1
done
echo "Kafka is ready"

# Wait for Redis cluster
echo "Waiting for Redis cluster..."
counter=0
until redis-cli -h redis-cluster -p 7000 cluster info 2>/dev/null | grep -q "cluster_slots_assigned:16384"; do
    counter=$((counter + 1))
    if [ $counter -ge $timeout ]; then
        echo "Error: Redis cluster did not become ready in time"
        exit 1
    fi
    sleep 1
done
echo "Redis cluster is ready"

# Run migrations
if [ -f ".venv/bin/snuba" ]; then
    echo "Running Snuba migrations..."
    .venv/bin/snuba migrations migrate --force
    echo "Migrations complete"
else
    echo "Warning: snuba CLI not found, skipping migrations"
fi

echo "=== All services ready ==="
