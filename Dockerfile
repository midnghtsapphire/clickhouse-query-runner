FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/

RUN uv pip install --system --no-cache .

ENTRYPOINT ["clickhouse-query-runner"]
CMD ["--help"]
