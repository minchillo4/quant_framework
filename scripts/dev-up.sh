#!/bin/bash
# scripts/dev-up.sh - Start development environment

set -e

echo "🚀 Starting Development Environment..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Set environment
export MNEMO_ENV=dev

# Check if .env.dev exists
if [ ! -f .env.dev ]; then
    echo "❌ Error: .env.dev file not found!"
    echo "   Create it from .env.example and configure for development"
    exit 1
fi

# Load environment variables for validation
echo "📋 Loading environment variables from .env.dev..."
set -a
source .env.dev
set +a

# Build and start services
echo ""
echo "📦 Building images (using .env.dev)..."
docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml build

echo ""
echo "🔧 Starting services (using .env.dev)..."
docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml up -d

echo ""
echo "⏳ Waiting for services to be healthy..."
sleep 5

# Show service status
echo ""
echo "📊 Service Status:"
docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml ps

echo ""
echo "✅ Development environment is starting up!"
echo ""
echo "📝 Useful commands:"
echo "   View logs:       docker compose -f docker-compose.yml -f docker-compose.dev.yml logs -f"
echo "   Stop services:   ./scripts/dev-down.sh"
echo "   Rebuild:         ./scripts/dev-rebuild.sh"
echo ""
echo "🌐 Access points:"
echo "   Airflow UI:      http://localhost:8080 (admin/admin)"
echo "   API:             http://localhost:8000"
echo "   Grafana:         http://localhost:3000 (admin/admin)"
echo "   pgAdmin:         http://localhost:5050"
echo "   MinIO Console:   http://localhost:9001 (admin/password123)"
echo ""
