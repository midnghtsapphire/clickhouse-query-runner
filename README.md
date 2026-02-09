# clickhouse-query-runner

A CLI tool that executes SQL queries from a file against a ClickHouse cluster, running queries in parallel with round-robin distribution across nodes, checkpointing progress in Valkey, and providing a rich terminal UI showing both batch-level and query-level progress.

## Features

- Parallel query execution with configurable concurrency
- Round-robin distribution across cluster nodes
- Checkpoint/resume via Valkey for fault tolerance
- Rich progress display with per-query monitoring
- Dry-run mode for query validation

## Installation

### uvx (Recommended)

Run directly without installing using [uv](https://docs.astral.sh/uv/):

```bash
uvx clickhouse-query-runner queries.sql
```

Or install as a persistent tool:

```bash
uv tool install clickhouse-query-runner
clickhouse-query-runner queries.sql
```

### Docker

Build the image:

```bash
docker build -t clickhouse-query-runner .
```

Run with environment variables and a local SQL file:

```bash
docker run --rm \
  -e CLICKHOUSE_HOST=clickhouse.example.com \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=secret \
  -e CLICKHOUSE_DATABASE=mydb \
  -v $(pwd)/queries.sql:/app/queries.sql \
  clickhouse-query-runner /app/queries.sql
```

Pass additional options after the image name:

```bash
docker run --rm \
  -e CLICKHOUSE_HOST=node1.example.com,node2.example.com \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=secret \
  -e CLICKHOUSE_DATABASE=mydb \
  -e VALKEY_URL=redis://valkey:6379/0 \
  -v $(pwd)/queries.sql:/app/queries.sql \
  clickhouse-query-runner --concurrency 4 /app/queries.sql
```

To use with `docker compose`, add the service to your `compose.yaml`:

```yaml
services:
  query-runner:
    build: .
    environment:
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: secret
      CLICKHOUSE_DATABASE: mydb
      VALKEY_URL: redis://valkey:6379/0
    volumes:
      - ./queries.sql:/app/queries.sql
    command: ["/app/queries.sql"]
    depends_on:
      - clickhouse
      - valkey
```

### Development

```bash
git clone https://github.com/gmr/clickhouse-query-runner.git
cd clickhouse-query-runner
uv sync --group dev
```

To run the tool during development:

```bash
uv run clickhouse-query-runner queries.sql
```

## Quick Start

```bash
# Set connection environment variables
export CLICKHOUSE_HOST=clickhouse.example.com
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=secret
export CLICKHOUSE_DATABASE=mydb

# Run queries from a file
uvx clickhouse-query-runner queries.sql

# With explicit options
uvx clickhouse-query-runner \
  --host node1.example.com,node2.example.com \
  --concurrency 4 \
  --valkey-url redis://valkey:6379/0 \
  queries.sql
```

## Command Reference

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--host` | `CLICKHOUSE_HOST` | (required) | ClickHouse hostname(s), comma-separated |
| `--port` | `CLICKHOUSE_PORT` | `9440` | ClickHouse server port |
| `--database` | `CLICKHOUSE_DATABASE` | (required) | Database name |
| `--user` | `CLICKHOUSE_USER` | (required) | Username |
| `--password` | `CLICKHOUSE_PASSWORD` | (required) | Password |
| `--secure` | `CLICKHOUSE_SECURE` | `true` | Use secure connection |
| `--concurrency` | | `2` | Max parallel queries |
| `--run-id` | | (auto) | Override run ID |
| `--valkey-url` | `VALKEY_URL` | `redis://localhost:6379/0` | Valkey URL |
| `--checkpoint-ttl` | | `604800` | Checkpoint TTL in seconds |
| `--poll-interval` | | `0.5` | Progress poll interval |
| `--cancel-on-failure` | | `false` | Cancel in-flight on failure |
| `--dry-run` | | `false` | Parse without executing |
| `--reset` | | `false` | Clear checkpoints and exit |
| `--verbose` | | `false` | Debug logging |

## How It Works

1. **Parse** - Split the SQL file into individual statements
2. **Checkpoint** - Load completed query hashes from Valkey, skip already-done queries
3. **Dispatch** - Send queries to nodes via round-robin, up to concurrency limit
4. **Monitor** - Poll `system.processes` for per-query progress
5. **Record** - Checkpoint each completed query to Valkey

## Architecture

```
src/clickhouse_query_runner/
├── __init__.py        # Package initialization
├── cli.py             # Entry point, arg parsing
├── runner.py          # Core async execution engine
├── checkpoint.py      # Valkey checkpoint management
├── parser.py          # SQL file parsing
├── progress.py        # Rich progress display
└── settings.py        # Pydantic settings model
```

## Code Quality

```bash
uv run ruff check src/
uv run ruff format --check src/
```

## License

BSD 3-Clause License. See [LICENSE](LICENSE) for details.
