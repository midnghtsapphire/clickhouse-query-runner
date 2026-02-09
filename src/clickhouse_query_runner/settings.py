"""Configuration management for ClickHouse query runner."""

from __future__ import annotations

import pydantic
import pydantic_settings


class RunnerSettings(pydantic_settings.BaseSettings):
    """Application settings with environment variable support."""

    model_config = pydantic_settings.SettingsConfigDict(
        cli_parse_args=True,
        cli_prog_name='clickhouse-query-runner',
        env_prefix='CLICKHOUSE_',
    )
    host: str = pydantic.Field(
        description='ClickHouse server hostname(s), comma-separated'
    )
    port: int = pydantic.Field(
        default=9440, description='ClickHouse server port'
    )
    database: str = pydantic.Field(description='Database name')
    secure: bool = pydantic.Field(
        default=True, description='Use secure connection'
    )
    user: str = pydantic.Field(description='Username for authentication')
    password: pydantic.SecretStr = pydantic.Field(
        description='Password for authentication'
    )
    concurrency: int = pydantic.Field(
        default=2, description='Max parallel queries'
    )
    run_id: str | None = pydantic.Field(
        default=None,
        description='Override the auto-generated run ID',
        alias='run-id',
    )
    valkey_url: str = pydantic.Field(
        default='redis://localhost:6379/0',
        description='Valkey connection URL',
        alias='valkey-url',
        validation_alias=pydantic.AliasChoices('VALKEY_URL', 'valkey-url'),
    )
    checkpoint_ttl: int = pydantic.Field(
        default=604800,
        description='Checkpoint expiry in seconds',
        alias='checkpoint-ttl',
    )
    poll_interval: float = pydantic.Field(
        default=0.5,
        description='Progress polling interval in seconds',
        alias='poll-interval',
    )
    cancel_on_failure: bool = pydantic.Field(
        default=False,
        description='Cancel in-flight queries on failure',
        alias='cancel-on-failure',
    )
    dry_run: bool = pydantic.Field(
        default=False,
        description='Parse and display queries without executing',
        alias='dry-run',
    )
    reset: bool = pydantic.Field(
        default=False, description='Clear all checkpoints for the run and exit'
    )
    verbose: bool = pydantic.Field(
        default=False, description='Enable debug logging'
    )
    query_file: pydantic_settings.CliPositionalArg[str] = pydantic.Field(
        description='Path to the SQL file containing queries'
    )

    @pydantic.field_validator('concurrency')
    @classmethod
    def validate_concurrency(cls, value: int) -> int:
        """Validate that concurrency is at least 1."""
        if value < 1:
            raise ValueError('concurrency must be at least 1')
        return value
