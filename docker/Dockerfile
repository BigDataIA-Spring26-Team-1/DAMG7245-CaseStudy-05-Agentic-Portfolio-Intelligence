FROM python:3.12-slim

WORKDIR /workspace

# Install system dependencies (Snowflake + build tools)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --uid 10001 appuser

# Install Poetry and application dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir poetry==2.2.1

COPY pyproject.toml poetry.lock README.md /workspace/
COPY pe-org-air-platform /workspace/pe-org-air-platform

RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi --without dev --with backend

RUN chown -R appuser:appuser /workspace

USER appuser
WORKDIR /workspace/pe-org-air-platform

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/openapi.json || exit 1

EXPOSE 8000

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
