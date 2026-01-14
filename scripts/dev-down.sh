#!/bin/bash
# scripts/dev-down.sh - Stop development environment

set -e

echo "🛑 Stopping Development Environment..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Set environment
export MNEMO_ENV=dev

# Stop services
docker compose -f docker-compose.yml -f docker-compose.dev.yml down

echo ""
echo "✅ Development environment stopped!"
echo ""
echo "💡 To remove volumes as well, run:"
echo "   docker compose -f docker-compose.yml -f docker-compose.dev.yml down -v"
echo ""
