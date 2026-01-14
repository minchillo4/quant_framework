#!/bin/bash
# scripts/prod-down.sh - Stop production environment

set -e

echo "🛑 Stopping Production Environment..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Set environment
export MNEMO_ENV=prod

# Confirmation prompt
read -p "⚠️  Are you sure you want to stop PRODUCTION services? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "❌ Operation cancelled"
    exit 1
fi

# Stop services
docker compose -f docker-compose.yml -f docker-compose.prod.yml down

echo ""
echo "✅ Production environment stopped!"
echo ""
echo "⚠️  IMPORTANT: Volumes are preserved."
echo "   To remove volumes (⚠️  DATA LOSS), run:"
echo "   docker compose -f docker-compose.yml -f docker-compose.prod.yml down -v"
echo ""
