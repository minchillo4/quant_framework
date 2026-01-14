#!/bin/bash
# scripts/prod-up.sh - Start production environment

set -e

echo "🚀 Starting Production Environment..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Set environment
export MNEMO_ENV=prod

# Check if .env.prod exists
if [ ! -f .env.prod ]; then
    echo "❌ Error: .env.prod file not found!"
    echo "   Create it from .env.example and configure for production"
    exit 1
fi

# Validate critical secrets
echo "🔒 Validating production secrets..."
if grep -q "CHANGE_ME" .env.prod; then
    echo "❌ Error: Found CHANGE_ME placeholders in .env.prod"
    echo "   Please update all secrets before deploying to production!"
    exit 1
fi

# Build and start services
echo ""
echo "📦 Building production images..."
docker compose -f docker-compose.yml -f docker-compose.prod.yml build

echo ""
echo "🔧 Starting services..."
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

echo ""
echo "⏳ Waiting for services to be healthy..."
sleep 10

# Show service status
echo ""
echo "📊 Service Status:"
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps

echo ""
echo "✅ Production environment is starting up!"
echo ""
echo "⚠️  Production Checklist:"
echo "   ☐ All secrets rotated and secured"
echo "   ☐ Backups configured"
echo "   ☐ Monitoring and alerting set up"
echo "   ☐ SSL/TLS certificates configured"
echo "   ☐ Firewall rules in place"
echo "   ☐ Resource limits configured"
echo ""
