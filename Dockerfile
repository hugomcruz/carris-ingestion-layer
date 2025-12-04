# Multi-stage build for minimal image size
FROM python:3.12-slim AS builder

WORKDIR /build

# Install only necessary build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and build wheels
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip wheel && \
    pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# Final stage - minimal runtime image
FROM python:3.12-slim

WORKDIR /app

# Install only runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get purge -y --auto-remove

# Copy and install wheels from builder
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/* && \
    rm -rf /wheels /root/.cache

# Copy only application code (no requirements.txt needed)
COPY src/ ./src/

# Create non-root user
RUN useradd -m -u 1000 -s /bin/false appuser && \
    chown -R appuser:appuser /app

USER appuser

# Expose port
EXPOSE 5000

# Optimized health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=2 \
    CMD python -c "import httpx; httpx.get('http://localhost:5000/health', timeout=3).raise_for_status()" || exit 1

# Run with optimizations
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "5000", "--workers", "1", "--log-level", "info"]
