# ---------- Build stage ----------
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Build wheels
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps -r requirements.txt -w /wheels


# ---------- Runtime stage ----------
FROM python:3.12-slim

WORKDIR /app

# Create non-root user
RUN useradd -m appuser

# Install runtime dependencies only
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/* \
    && rm -rf /wheels

# Copy application code
COPY simpldb/ ./simpldb/
COPY webapp/ ./webapp/

# Create persistent data directory
RUN mkdir -p /app/data \
    && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV DATABASE_DIR=/app/data

# Expose app port
EXPOSE 8000

# Health check (VS Code + Docker safe)
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')"

# Run with gunicorn
CMD ["gunicorn", "-w", "2", "--threads", "2", "-b", "0.0.0.0:8000", "webapp.app:app"]
