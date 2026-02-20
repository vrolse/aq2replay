FROM python:3.12-slim

# System deps for Pillow (PCX/PNG support only — no heavy libs needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libzstd1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer-cached separately from app code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY web/ ./web/

# Data directories are bind-mounted at runtime (see docker-compose.yml).
# Create them here so the app starts even if mounts are missing.
RUN mkdir -p /app/mvd2 /app/bsp /app/textures /app/cache/topview

ENV PYTHONUNBUFFERED=1

EXPOSE 5000

CMD ["python", "web/app.py"]
