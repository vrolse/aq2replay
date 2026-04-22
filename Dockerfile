FROM python:3.12-slim

# System deps for Pillow (PCX/PNG support only — no heavy libs needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libzstd1 wget \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer-cached separately from app code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY web/ ./web/

# Copy operator-facing diagnostic scripts (run via `docker compose exec`).
COPY check_parse_status.py ./

# Data directories are bind-mounted at runtime (see docker-compose.yml).
# Create them here so the app starts even if mounts are missing.
RUN mkdir -p /app/mvd2 /app/bsp /app/textures \
             /app/players /app/models \
             /app/cache/topview /app/cache/md2 /app/cache/mesh /app/cache/textures \
             /var/lib/aq2replay

ENV PYTHONUNBUFFERED=1

EXPOSE 5000

HEALTHCHECK --interval=5m --timeout=5s --start-period=60s --retries=2 \
  CMD wget -qO /dev/null http://127.0.0.1:5000/healthz || exit 1

# Production default: run Flask app behind Gunicorn (WSGI).
# Runtime tuning can be overridden with env vars in compose.
CMD ["sh", "-c", "gunicorn --chdir /app/web --bind 0.0.0.0:5000 --workers ${GUNICORN_WORKERS:-1} --threads ${GUNICORN_THREADS:-8} --timeout ${GUNICORN_TIMEOUT:-180} --graceful-timeout ${GUNICORN_GRACEFUL_TIMEOUT:-30} --access-logfile - --error-logfile - app:app"]
