"""Valkey checkpoint management for ClickHouse query runner."""

from __future__ import annotations

import hashlib
import json
import logging
import time
import typing

import valkey.asyncio as avalkey
import valkey.exceptions

LOGGER = logging.getLogger(__name__)

DEFAULT_TTL = 604800  # 7 days


class CheckpointManager:
    """Manages query execution checkpoints in Valkey."""

    def __init__(
        self,
        valkey_url: str,
        run_id: str | None,
        file_path: str,
        ttl: int = DEFAULT_TTL,
    ) -> None:
        self.client: avalkey.Valkey | None = None
        self.valkey_url = valkey_url
        self.run_id = (
            run_id or hashlib.sha256(file_path.encode()).hexdigest()[:16]
        )
        self.ttl = ttl
        self._last_connected: float = 0.0
        self._available = True

    async def connect(self) -> None:
        """Establish connection to Valkey."""
        self.client = avalkey.from_url(self.valkey_url)
        try:
            await self.client.ping()
            self._last_connected = time.monotonic()
            LOGGER.debug('Connected to Valkey at %s', self.valkey_url)
        except (OSError, valkey.exceptions.ValkeyError):
            LOGGER.warning(
                'Could not connect to Valkey at %s, '
                'running without checkpointing',
                self.valkey_url,
            )
            self._available = False

    async def close(self) -> None:
        """Close Valkey connection."""
        if self.client is not None:
            await self.client.aclose()

    def _key(self, query_hash: str) -> str:
        """Build the checkpoint key."""
        return f'cqr:{self.run_id}:{query_hash}'

    async def load_completed(self) -> set[str]:
        """Load all completed query hashes for this run.

        Returns:
            Set of query hashes with status 'completed'.
        """
        if not self._available:
            return set()
        completed: set[str] = set()
        try:
            pattern = f'cqr:{self.run_id}:*'
            cursor = 0
            while True:
                cursor, keys = await self.client.scan(
                    cursor, match=pattern, count=100
                )
                for key in keys:
                    raw = await self.client.get(key)
                    if raw:
                        data = json.loads(raw)
                        if data.get('status') == 'completed':
                            # Extract hash from key
                            parts = key.decode().split(':')
                            if len(parts) == 3:
                                completed.add(parts[2])
                if cursor == 0:
                    break
            self._last_connected = time.monotonic()
        except (OSError, valkey.exceptions.ValkeyError):
            LOGGER.warning('Failed to load checkpoints from Valkey')
            self._check_availability()
        return completed

    async def mark_completed(
        self,
        query_hash: str,
        node: str,
        started_at: str,
        completed_at: str,
        rows_read: int,
        elapsed: float,
    ) -> None:
        """Write a completion checkpoint."""
        await self._write(
            query_hash,
            {
                'status': 'completed',
                'started_at': started_at,
                'completed_at': completed_at,
                'node': node,
                'rows_read': rows_read,
                'elapsed': round(elapsed, 2),
            },
        )

    async def mark_failed(
        self, query_hash: str, node: str, started_at: str, error: str
    ) -> None:
        """Write a failure checkpoint."""
        await self._write(
            query_hash,
            {
                'status': 'failed',
                'started_at': started_at,
                'node': node,
                'error': error,
            },
        )

    async def reset(self) -> int:
        """Clear all checkpoints for this run.

        Returns:
            Number of keys deleted.
        """
        if not self._available:
            LOGGER.warning('Valkey unavailable, cannot reset checkpoints')
            return 0
        deleted = 0
        try:
            pattern = f'cqr:{self.run_id}:*'
            cursor = 0
            while True:
                cursor, keys = await self.client.scan(
                    cursor, match=pattern, count=100
                )
                if keys:
                    deleted += await self.client.delete(*keys)
                if cursor == 0:
                    break
            LOGGER.info(
                'Cleared %d checkpoints for run %s', deleted, self.run_id
            )
        except (OSError, valkey.exceptions.ValkeyError):
            LOGGER.warning('Failed to reset checkpoints')
        return deleted

    async def _write(
        self, query_hash: str, data: dict[str, typing.Any]
    ) -> None:
        """Write checkpoint data to Valkey."""
        if not self._available:
            return
        key = self._key(query_hash)
        try:
            await self.client.setex(key, self.ttl, json.dumps(data))
            self._last_connected = time.monotonic()
        except (OSError, valkey.exceptions.ValkeyError):
            LOGGER.warning(
                'Failed to write checkpoint for %s', query_hash[:12]
            )
            self._check_availability()

    def _check_availability(self) -> None:
        """Mark Valkey as unavailable if down too long."""
        if (
            self._available
            and self._last_connected
            and (time.monotonic() - self._last_connected) > 30
        ):
            self._available = False
            LOGGER.error(
                'Valkey has been unreachable for 30s, stopping execution'
            )
            raise OSError('Valkey connection lost for >30s')
