#!/bin/bash
# scripts/dev-rebuild.sh - Rebuild development images

set -e

echo "🔨 Rebuilding Development Images..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Set environment
export MNEMO_ENV=dev

# Rebuild without cache
echo "Building images without cache..."
docker compose -f docker-compose.yml -f docker-compose.dev.yml build --no-cache

echo ""
echo "✅ Images rebuilt successfully!"
echo ""
echo "💡 To restart with new images:"
echo "   ./scripts/dev-down.sh"
echo "   ./scripts/dev-up.sh"
echo ""
