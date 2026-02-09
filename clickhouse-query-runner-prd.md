# PRD: clickhouse-query-runner

**Author:** Gavin M. Roy
**Date:** 2026-02-09
**Status:** Draft

## Problem

Backfilling materialized views in ClickHouse requires executing large batches of ordered SQL queries across cluster nodes. Manually running these queries is error-prone, provides no visibility into progress, and offers no recovery mechanism when failures occur mid-batch.

## Goal

A CLI tool that executes SQL queries from a file against a ClickHouse cluster, running queries in parallel with round-robin distribution across nodes, checkpointing progress in Valkey, and providing rich terminal UI showing both batch-level and query-level progress.

## Non-Goals

- Generation of the SQL query files (handled elsewhere)
- Schema management or migration tooling
- Interactive query editing
- Support for non-ClickHouse databases

## Architecture

```
┌─────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│  SQL File   │────▶│  clickhouse-query-   │────▶│  ClickHouse Cluster │
│  (ordered)  │     │  runner              │     │  ├─ node-0-0        │
└─────────────┘     │                      │     │  └─ node-0-1        │
                    │ async query dispatch │     └─────────────────────┘
                    │ round-robin nodes    │
                    │ checkpoint to valkey │     ┌─────────────────────┐
                    │ rich progress UI     │────▶│  Valkey             │
                    └──────────────────────┘     │  (checkpoints)      │
                                                 └─────────────────────┘
```

### Async Design

Use `asyncio` as the concurrency model. The `clickhouse-driver` package supports an async client via `clickhouse_driver.Client` used within `asyncio.to_thread`, or alternatively use `asynch` (the native async ClickHouse driver). Evaluate `asynch` first as it avoids thread-pool wrapping.

### Connection Management

Maintain a persistent async connection per node. Assign queries to nodes via round-robin using an `itertools.cycle` over the node list.

Cluster nodes should be specified with a comma separated list.

Connection parameters (host, port, database, credentials) should follow the same pattern as `clickhouse-optimizer`: pydantic-settings with env var support (`CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_PORT` (default 9440), `CLICKHOUSE_SECURE` (default true)).

## Functional Requirements

### 1. Query File Parsing

Accept a file path as a positional CLI argument. Parse the file into individual SQL statements delimited by `;` (accounting for semicolons inside string literals and comments). Preserve statement order as the execution order.

Each query gets a stable identifier derived from `hashlib.sha256(query.strip().encode()).hexdigest()`. This hash is used as the checkpoint key.

### 2. Checkpointing (Valkey)

Use `valkey-py` (or `redis-py` with Valkey compatibility) for checkpoint storage.

**Key schema:** `cqr:{run_id}:{query_hash}` where `run_id` is derived from the SHA-256 of the file path (or provided via `--run-id`).

**Value:** JSON object:

```json
{
  "status": "completed",
  "started_at": "2026-02-09T14:30:00Z",
  "completed_at": "2026-02-09T14:30:12Z",
  "node": "node-0-0.fqdn",
  "rows_read": 1500000,
  "elapsed": 12.34
}
```

**Behavior on startup:**

1. Load all checkpoint keys for the run.
2. Skip queries whose hash has `status: completed`.
3. Log the number of skipped queries at startup.

**TTL:** Checkpoint keys expire after 7 days (configurable via `--checkpoint-ttl`).

**Valkey connection:** Configurable via `--valkey-url` (default: `redis://localhost:6379/0`).

### 3. Query Execution

**Parallelism:** Execute up to N queries concurrently (configurable via `--concurrency`, default: 2, one per node). Queries are dispatched in file order but may complete out of order due to varying execution times.

**Round-robin dispatch:** Each new query dispatched goes to the next node in the cycle. This distributes load evenly without requiring health checks or query-time-based balancing.

**Ordering guarantee:** Queries are _dispatched_ in file order. The Nth query is dispatched before the (N+1)th query, but the Nth query is not required to complete before the (N+1)th is dispatched (up to the concurrency limit). If strict serial ordering is required, use `--concurrency 1`.

**Failure behavior:** When any query fails:

1. Stop dispatching new queries immediately.
2. Allow in-flight queries to complete (or cancel them if `--cancel-on-failure` is set).
3. Print the failed query text, the node it ran on, and the full error message.
4. Write a `failed` checkpoint with the error details.
5. Exit with a non-zero status code.

On restart, the failed query will be retried (its checkpoint status is `failed`, not `completed`).

### 4. Progress Monitoring

#### 4a. Batch Progress (Rich)

Use `rich.progress.Progress` to display an overall progress bar:

```
Backfill ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  64% 320/500 queries  12.3s/q  ETA 0:37:00
```

Columns:

- Task description
- Progress bar
- Percentage complete
- `{completed}/{total}` queries
- Rolling average time per query (last 10 queries)
- ETA based on rolling average

#### 4b. Active Query Progress (Rich Live)

Below the batch progress bar, display a live-updating table of currently executing queries showing per-query progress similar to the `clickhouse-client` display.

```
Node          Offset    Progress                          Rows Read     Progress   Elapsed
primary-0-0     10      ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  1.2M / 4.8M    25.0%     0:00:08
primary-0-1     11      ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  890K / 2.1M    42.3%     0:00:12
primary-0-0     12      ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  1.2M / 4.8M    25.0%     0:00:08
primary-0-1     13      ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  890K / 2.1M    42.3%     0:00:12
```

**Implementation:** Poll `system.processes` on each node at a configurable interval (`--poll-interval`, default: 0.5s) to retrieve:

```sql
SELECT
    query_id,
    read_rows,
    total_rows_to_read,
    elapsed,
    round(100.0 * read_rows / nullif(total_rows_to_read, 0), 1)
        AS progress_percent
  FROM system.processes
 WHERE query_id = {query_id:String}
```

Use `rich.live.Live` with a `rich.table.Table` rendered inside the same `Live` context as the progress bar, following the pattern from `clickhouse-optimizer`.

### 5. CLI Interface

```
Usage: clickhouse-query-runner [OPTIONS] QUERY_FILE

Arguments:
  QUERY_FILE              Path to the SQL file containing queries

Options:
  --concurrency N         Max parallel queries (default: 2)
  --run-id TEXT           Override the auto-generated run ID
  --valkey-url URL        Valkey connection URL
                          (default: redis://localhost:6379/0)
  --checkpoint-ttl SECS   Checkpoint expiry in seconds
                          (default: 604800)
  --poll-interval SECS    Progress polling interval
                          (default: 0.5)
  --cancel-on-failure     Cancel in-flight queries on failure
  --dry-run               Parse and display queries without
                          executing
  --reset                 Clear all checkpoints for the run
                          and exit
  --verbose               Enable debug logging
```

Connection options follow `clickhouse-optimizer` conventions (env vars with CLI overrides).

## Configuration

Use `pydantic-settings` with the following precedence (highest to lowest):

1. CLI arguments
2. Environment variables (`CH_HOST`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE`, `CH_PORT`, `CH_SECURE`, `VALKEY_URL`)
3. Defaults

## Project Structure

```
clickhouse-query-runner/
├── pyproject.toml
├── src/
│   └── clickhouse_query_runner/
│       ├── __init__.py
│       ├── cli.py              # Entry point, arg parsing
│       ├── runner.py           # Core async execution engine
│       ├── checkpoint.py       # Valkey checkpoint management
│       ├── parser.py           # SQL file parsing
│       ├── progress.py         # Rich progress display
│       └── settings.py         # Pydantic settings model
```

## Dependencies

| Package                          | Purpose                                                                         |
| -------------------------------- | ------------------------------------------------------------------------------- |
| `asynch`                         | Async ClickHouse driver (evaluate vs `clickhouse-driver` + `asyncio.to_thread`) |
| `pydantic` / `pydantic-settings` | Configuration with CLI + env var support                                        |
| `rich`                           | Progress bars, live display, console output                                     |
| `valkey-py`                      | Checkpoint storage (async via `valkey.asyncio`)                                 |

## Error Handling

| Scenario                      | Behavior                                                                                                                                            |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Query syntax error            | Stop dispatch, report query text + error, exit 1                                                                                                    |
| Connection lost (single node) | Stop dispatch, report node + error, exit 1                                                                                                          |
| Connection lost (Valkey)      | Log warning, continue execution without checkpointing. On next query completion, retry Valkey write. If Valkey stays down for 30s, stop with error. |
| File not found                | Exit immediately with error message                                                                                                                 |
| Empty query file              | Exit 0 with informational message                                                                                                                   |
| All queries checkpointed      | Exit 0 with "nothing to do" message                                                                                                                 |
| Keyboard interrupt            | Cancel in-flight queries, flush checkpoint state, exit 130                                                                                          |

## Logging

Use `logging` with `rich.logging.RichHandler` (consistent with `clickhouse-optimizer`). Default level `INFO`, `--verbose` enables `DEBUG`. Use `LOGGING.debug` for debug output per project conventions.

## Future Considerations

- Dynamic node discovery from `system.clusters`
- Adaptive concurrency based on cluster load
- Query-time-weighted node assignment instead of round-robin
- Web UI for monitoring long-running backfills
- Retry with configurable backoff on transient failures
