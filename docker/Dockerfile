FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

# Install system dependencies - ADD liblz4-dev here
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    liblz4-dev \
    liblz4-1 \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies

# Ensure the LZ4 Python bindings are available


# Create virtual environment
RUN uv venv -p python3.11
ENV PATH="/app/.venv/bin:$PATH"

# Copy requirements for caching
COPY pyproject.toml README.md ./
# Create src directory structure
RUN mkdir -p src/mnemo_quant src/mnemo_quant_api config scripts tests

# Install dependencies
RUN uv pip install -e .[dev]

# Install LZ4 Python package explicitly - ADD THIS SECTION
RUN uv pip install lz4

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY tests/ ./tests/
COPY src/mnemo_quant /opt/airflow/dags/mnemo_quant

# Set Python path
ENV PYTHONPATH="/app/src:$PYTHONPATH"

# Default command (overridden in docker-compose.yml)
CMD ["tail", "-f", "/dev/null"]