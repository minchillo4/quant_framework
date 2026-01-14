#!/bin/bash
# scripts/dev-logs.sh - View development logs

set -e

SERVICE=${1:-""}

if [ -z "$SERVICE" ]; then
    echo "📋 Following logs for all services..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker compose -f docker-compose.yml -f docker-compose.dev.yml logs -f --tail=50
else
    echo "📋 Following logs for: $SERVICE"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker compose -f docker-compose.yml -f docker-compose.dev.yml logs -f --tail=50 "$SERVICE"
fi
