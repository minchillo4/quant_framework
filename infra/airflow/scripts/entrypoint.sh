#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Starting Airflow Entrypoint..."

# Function to wait for database
wait_for_db() {
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Waiting for database to be ready..."
    
    until airflow db check-migrations 2>/dev/null; do
        if [ $attempt -eq $max_attempts ]; then
            echo "❌ Database not ready after $max_attempts attempts"
            return 1
        fi
        
        echo "⏳ Attempt $attempt/$max_attempts: Database not ready, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "✅ Database is ready!"
}

# Initialize database and create user if needed
initialize_airflow() {
    echo "🔧 Initializing Airflow..."
    
    # Check if database needs initialization
    if ! airflow db check-migrations 2>/dev/null; then
        echo "🏗️  Running database migrations..."
        airflow db migrate
    fi
    
    # Create default admin user if it doesn't exist
    if ! airflow users list | grep -q "${AIRFLOW_USERNAME:-admin}"; then
        echo "👤 Creating admin user..."
        airflow users create \
            --username "${AIRFLOW_USERNAME:-admin}" \
            --firstname Airflow \
            --lastname Admin \
            --role Admin \
            --email "${AIRFLOW_EMAIL:-admin@example.com}" \
            --password "${AIRFLOW_PASSWORD:-admin}"
    else
        echo "✅ Admin user already exists"
    fi
    
    echo "✅ Airflow initialization complete"
}

# Main execution
# Main execution
case "${1:-}" in
    webserver|scheduler|triggerer|worker)
        wait_for_db
        initialize_airflow
        exec airflow "$@"
        ;;
    bash|sh|python)
        # Run raw shell/python without prefixing with airflow
        # Example: bash -c "airflow db migrate && ..."
        exec "$1" "${@:2}"
        ;;
    version)
        exec airflow "$@"
        ;;
    init)
        # Run initialization and exit
        wait_for_db
        initialize_airflow
        echo "✅ Airflow initialization complete"
        exit 0
        ;;
    *)
        # For any other command, pass through to airflow
        exec airflow "$@"
        ;;
esac