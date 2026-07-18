#!/usr/bin/env bash
# Build and start the full relay-chain benchmark stack (Kafka cluster, Mongo
# replica set, producer, 3x consumer, dashboard).
#
# Usage: ./start.sh
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Building service images..."
docker compose build

echo "==> Starting the stack (waiting for healthchecks and init jobs)..."
docker compose up -d --wait --wait-timeout 180

docker compose ps
echo
echo "Dashboard: http://localhost:8085"
echo "Stop and clean up with: ./stop.sh"
