"""Core async execution engine for ClickHouse query runner."""

from __future__ import annotations

import asyncio
import datetime
import itertools
import logging
import time
import typing
import uuid

from asynch import connection as asynch_connection
from asynch import errors as asynch_errors

from clickhouse_query_runner import checkpoint, progress, settings

LOGGER = logging.getLogger(__name__)

PROGRESS_QUERY = """\
SELECT query_id,
       read_rows,
       total_rows_to_read,
       elapsed,
       round(100.0 * read_rows / nullif(total_rows_to_read, 0), 1)
           AS progress_percent
  FROM system.processes
 WHERE query_id = %(query_id)s"""


class QueryRunner:
    """Executes SQL queries against a ClickHouse cluster."""

    def __init__(self, runner_settings: settings.RunnerSettings) -> None:
        self.settings = runner_settings
        self.nodes = [h.strip() for h in runner_settings.host.split(',')]
        self.connections: dict[str, asynch_connection.Connection] = {}
        self.node_cycle = itertools.cycle(self.nodes)
        self.checkpoint_mgr = checkpoint.CheckpointManager(
            valkey_url=runner_settings.valkey_url,
            run_id=runner_settings.run_id,
            file_path=runner_settings.query_file,
            ttl=runner_settings.checkpoint_ttl,
        )
        self._failure: _QueryFailure | None = None
        self._stop_dispatch = False
        self._progress: progress.QueryProgress | None = None
        self._poll_tasks: set[asyncio.Task[None]] = set()

    async def connect(self) -> None:
        """Establish connections to all ClickHouse nodes."""
        for node in self.nodes:
            conn = asynch_connection.Connection(
                host=node,
                port=self.settings.port,
                database=self.settings.database,
                user=self.settings.user,
                password=(self.settings.password.get_secret_value()),
                secure=self.settings.secure,
            )
            await conn.connect()
            self.connections[node] = conn
            LOGGER.debug('Connected to ClickHouse node %s', node)
        await self.checkpoint_mgr.connect()

    async def close(self) -> None:
        """Close all connections."""
        for task in self._poll_tasks:
            task.cancel()
        for node, conn in self.connections.items():
            try:
                await conn.close()
            except OSError:
                LOGGER.debug('Error closing connection to %s', node)
        await self.checkpoint_mgr.close()

    async def run(self, queries: list[tuple[str, str]]) -> bool:
        """Execute queries with concurrency control.

        Args:
            queries: List of (query_hash, query_text) tuples.

        Returns:
            True if all queries completed successfully.
        """
        completed = await self.checkpoint_mgr.load_completed()
        pending = [
            (i, qh, qt)
            for i, (qh, qt) in enumerate(queries)
            if qh not in completed
        ]
        skipped = len(queries) - len(pending)
        if skipped:
            LOGGER.info('Skipping %d already-completed queries', skipped)

        if not pending:
            LOGGER.info('All queries already completed, nothing to do')
            return True

        self._progress = progress.QueryProgress(len(queries))
        if skipped:
            self._progress.mark_skipped(skipped)

        live_ctx = self._progress.start()
        semaphore = asyncio.Semaphore(self.settings.concurrency)
        tasks: list[asyncio.Task[None]] = []

        with live_ctx:
            for offset, query_hash, query_text in pending:
                if self._stop_dispatch:
                    break
                await semaphore.acquire()
                if self._stop_dispatch:
                    semaphore.release()
                    break
                node = next(self.node_cycle)
                task = asyncio.create_task(
                    self._execute_query(
                        node, offset, query_hash, query_text, semaphore
                    )
                )
                tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        # Cancel polling tasks
        for pt in self._poll_tasks:
            pt.cancel()

        return self._failure is None

    async def reset(self) -> int:
        """Clear all checkpoints for this run."""
        return await self.checkpoint_mgr.reset()

    async def _execute_query(
        self,
        node: str,
        offset: int,
        query_hash: str,
        query_text: str,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """Execute a single query on the specified node."""
        query_id = str(uuid.uuid4())
        started_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
        start_time = time.monotonic()

        if self._progress is not None:
            self._progress.mark_started(query_id, node, offset)

        # Start polling for progress
        poll_task = asyncio.create_task(self._poll_progress(node, query_id))
        self._poll_tasks.add(poll_task)

        try:
            conn = self.connections[node]
            async with conn.cursor() as cursor:
                cursor._query_id = query_id
                await cursor.execute(query_text)
                rows_read = cursor.rowcount or 0

            elapsed = time.monotonic() - start_time
            completed_at = datetime.datetime.now(tz=datetime.UTC).isoformat()

            await self.checkpoint_mgr.mark_completed(
                query_hash=query_hash,
                node=node,
                started_at=started_at,
                completed_at=completed_at,
                rows_read=rows_read,
                elapsed=elapsed,
            )

            if self._progress is not None:
                self._progress.mark_completed(query_id, elapsed)

            LOGGER.debug(
                'Query %d completed on %s in %.1fs', offset, node, elapsed
            )
        except (
            OSError,
            asynch_errors.ServerException,
            asynch_errors.UnexpectedPacketFromServerError,
        ) as err:
            elapsed = time.monotonic() - start_time
            self._stop_dispatch = True
            self._failure = _QueryFailure(
                query_text=query_text, node=node, error=str(err), offset=offset
            )
            await self.checkpoint_mgr.mark_failed(
                query_hash=query_hash,
                node=node,
                started_at=started_at,
                error=str(err),
            )
            if self._progress is not None:
                self._progress.mark_completed(query_id, elapsed)
            LOGGER.error('Query %d failed on %s: %s', offset, node, err)

            if self.settings.cancel_on_failure:
                await self._cancel_in_flight()
        finally:
            poll_task.cancel()
            self._poll_tasks.discard(poll_task)
            semaphore.release()

    async def _poll_progress(self, node: str, query_id: str) -> None:
        """Poll system.processes for query progress."""
        await asyncio.sleep(self.settings.poll_interval)
        try:
            while True:
                try:
                    conn = self.connections[node]
                    async with conn.cursor() as cursor:
                        await cursor.execute(
                            PROGRESS_QUERY, {'query_id': query_id}
                        )
                        row = cursor.fetchone()
                        if row and self._progress is not None:
                            self._progress.update_query(
                                query_id=query_id,
                                read_rows=row[1] or 0,
                                total_rows=row[2] or 0,
                                elapsed=row[3] or 0.0,
                                pct=row[4] or 0.0,
                            )
                except (  # noqa: S110
                    OSError,
                    asynch_errors.ServerException,
                    asynch_errors.UnexpectedPacketFromServerError,
                ):
                    pass  # Progress polling is best-effort
                await asyncio.sleep(self.settings.poll_interval)
        except asyncio.CancelledError:
            return

    async def _cancel_in_flight(self) -> None:
        """Cancel all in-flight queries on all nodes."""
        LOGGER.info('Cancelling in-flight queries')
        for node, conn in self.connections.items():
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        'KILL QUERY WHERE user = currentUser()'
                    )
            except OSError, asynch_errors.ServerException:
                LOGGER.debug('Error cancelling queries on %s', node)

    @property
    def failure(self) -> _QueryFailure | None:
        """Return failure details if a query failed."""
        return self._failure


class _QueryFailure(typing.NamedTuple):
    """Details of a failed query."""

    query_text: str
    node: str
    error: str
    offset: int
