#!/usr/bin/env bash
set -euo pipefail

echo "Initializing Bronze Layer (MinIO)..."

ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET="${MINIO_BUCKET_NAME:-bronze}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-admin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-password123}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"

echo "Using endpoint: $ENDPOINT"
echo "Using bucket: $BUCKET"

# Wait for MinIO
echo "Waiting for MinIO to be ready..."
for i in {1..30}; do
    if curl -s -f "${ENDPOINT}/minio/health/live" > /dev/null 2>&1; then
        echo "MinIO is ready"
        break
    fi
    echo "Attempt $i/10..."
    sleep 2
done

# Give MinIO a moment to fully initialize
sleep 2

# Configure mc client
echo "Configuring MinIO client..."
if mc alias set local "${ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" > /dev/null 2>&1; then
    echo "MC client configured"
else
    echo "Could not configure mc client, trying with --insecure flag..."
    mc alias set local "${ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" --insecure
fi

# Create main bronze bucket
echo "Creating bucket: ${BUCKET}..."
if mc ls "local/${BUCKET}" > /dev/null 2>&1; then
    echo "Bucket '${BUCKET}' already exists"
else
    if mc mb "local/${BUCKET}" --region="${MINIO_REGION}" > /dev/null 2>&1; then
        echo "Created bucket: ${BUCKET}"
    else
        echo "Could not create bucket, trying with --insecure flag..."
        mc mb "local/${BUCKET}" --region="${MINIO_REGION}" --insecure
        echo "Created bucket: ${BUCKET} (with --insecure)"
    fi
fi

# Create standardized folder structure
echo "Creating folder structure..."
FOLDERS=(
    "source=coinalyze/data_type=ohlc"
    "source=coinalyze/data_type=open_interest"
    "source=ccxt/data_type=ohlc"
    "source=ccxt/data_type=open_interest"
)

for folder in "${FOLDERS[@]}"; do
    echo "  Creating: ${folder}/"
    if echo -n "" | mc pipe "local/${BUCKET}/${folder}/.keep" > /dev/null 2>&1; then
        echo "    Created"
    else
        echo "    Trying with --insecure flag..."
        echo -n "" | mc pipe "local/${BUCKET}/${folder}/.keep" --insecure > /dev/null 2>&1 || true
        echo "    Created (with --insecure)"
    fi
done

# Set bucket policy
echo "Setting bucket policy..."
if mc policy set download "local/${BUCKET}" > /dev/null 2>&1; then
    echo "Bucket policy set"
else
    echo "Could not set policy, trying with --insecure flag..."
    mc policy set download "local/${BUCKET}" --insecure > /dev/null 2>&1 || true
    echo "Bucket policy set (with --insecure)"
fi

echo "Bronze layer initialization complete!"
echo "Bucket: ${BUCKET}"
echo "Console: http://localhost:9001"
echo "Endpoint: ${ENDPOINT}"
