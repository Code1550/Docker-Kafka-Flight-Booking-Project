# api-gateway/Dockerfile
#
# Multi-stage build for the API Gateway service.
#
# Stage 1 (builder) installs dependencies into an isolated layer.
# Stage 2 (runtime) copies only what is needed to run the app,
# keeping the final image lean and free of build tools.

# ──────────────────────────────────────────
# STAGE 1 — builder
# ──────────────────────────────────────────

FROM python:3.11-slim AS builder

# Prevents Python from writing .pyc files to disk (keeps the image clean)
ENV PYTHONDONTWRITEBYTECODE=1

# Ensures Python output is sent straight to the terminal without buffering
# (critical for seeing logs in real time inside Docker)
ENV PYTHONUNBUFFERED=1

WORKDIR /build

# Copy requirements first — before any app code.
# Docker caches each instruction as a layer. By copying requirements.txt
# before the rest of the source, the pip install layer is only invalidated
# when requirements.txt actually changes, not on every code edit.
COPY requirements.txt .

# Upgrade pip, then install all dependencies into a local directory.
# --no-cache-dir  → don't store the pip download cache inside the image
# --upgrade pip   → avoids deprecation warnings from old pip versions
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt


# ──────────────────────────────────────────
# STAGE 2 — runtime
# ──────────────────────────────────────────

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Run as a non-root user for security.
# If the container is ever compromised, the attacker has limited privileges.
RUN addgroup --system appgroup && \
    adduser  --system --ingroup appgroup appuser

WORKDIR /app

# Copy installed packages from the builder stage into the system site-packages.
# This is what makes the multi-stage build work — no pip needed at runtime.
COPY --from=builder /install /usr/local

# Copy the application source code
COPY app/ ./app/

# The shared/ package is NOT copied here — it is mounted at runtime via
# the docker-compose volume:
#   volumes:
#     - ./shared:/app/shared
# This means changes to shared/schemas.py are reflected immediately
# without rebuilding the image.

# Switch to the non-root user before the process starts
USER appuser

# Expose the port FastAPI will listen on.
# This is documentation — the actual port binding happens in docker-compose.
EXPOSE 8000

# Health check so Docker (and docker-compose depends_on) knows when the
# service is ready to accept traffic. Hits the /health endpoint every 30s.
# --fail makes curl return a non-zero exit code on HTTP 4xx/5xx responses.
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD ["sh", "-c", "curl -f http://localhost:8000/health || exit 1"]

# Start the FastAPI app with uvicorn.
#
# --host 0.0.0.0    → listen on all interfaces inside the container
# --port 8000       → must match EXPOSE above and docker-compose ports
# --reload          → auto-restart on code changes (remove in production)
# --workers 1       → single worker is fine for local dev; increase for prod
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--reload", \
     "--workers", "1"]
