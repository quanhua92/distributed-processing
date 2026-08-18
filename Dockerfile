# syntax=docker/dockerfile:1.7
# Single image for all roles (api, worker, audit); role selected via `--role` arg.

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /app

# uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:0.7.20 /uv /usr/local/bin/uv

# Copy project definition & sources
COPY pyproject.toml README.md ./
COPY src ./src
COPY dashboard ./dashboard
COPY scripts ./scripts

# Install runtime dependencies + the project itself
RUN uv pip install --system -e .

ENTRYPOINT ["python", "-m", "distributed_processing"]
CMD ["--role", "worker"]
