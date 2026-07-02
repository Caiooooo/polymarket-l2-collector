FROM python:3.11-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

# Install dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application
COPY polymarket_l2_collector/ polymarket_l2_collector/

# Create data and log directories
RUN mkdir -p data logs

ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1

ENTRYPOINT ["uv", "run", "python", "-m", "polymarket_l2_collector"]
