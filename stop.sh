#!/usr/bin/env bash
# Stop the stack and fully clean up everything this project created: all
# containers, the compose network, anonymous volumes (Kafka/Mongo data - the
# TTL/retention config assumes a fresh volume on every run), and the images
# built locally for producer/consumer/dashboard-service.
#
# Does NOT remove pulled base images (apache/kafka, mongo:7) - those are
# shared, reusable across runs, and not project-specific storage.
#
# Usage: ./stop.sh
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Stopping and removing containers, network, volumes, and built images..."
docker compose down --volumes --remove-orphans --rmi local

echo "==> Cleanup complete."
