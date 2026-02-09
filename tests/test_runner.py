"""Tests for the async execution engine."""

from __future__ import annotations

import asyncio
import contextlib
import unittest
from unittest import mock

from asynch import errors as asynch_errors

from clickhouse_query_runner import runner


def _make_settings(**overrides: object) -> mock.MagicMock:
    """Create a mock RunnerSettings."""
    defaults = {
        'host': 'node1,node2',
        'port': 9440,
        'database': 'default',
        'user': 'user',
        'password': mock.MagicMock(get_secret_value=lambda: 'pass'),
        'secure': True,
        'concurrency': 2,
        'run_id': 'test-run',
        'query_file': 'test.sql',
        'valkey_url': 'redis://localhost:6379/0',
        'checkpoint_ttl': 604800,
        'poll_interval': 0.01,
        'cancel_on_failure': False,
        'dry_run': False,
        'reset': False,
        'verbose': False,
    }
    defaults.update(overrides)
    s = mock.MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


class _MockCursor:
    """Async context manager cursor mock."""

    def __init__(
        self,
        rowcount: int = 0,
        execute_side_effect: BaseException | None = None,
        fetchone_return: object = None,
    ) -> None:
        self.rowcount = rowcount
        self._execute_side_effect = execute_side_effect
        self._fetchone_return = fetchone_return
        self._query_id = ''
        self.execute = mock.AsyncMock(side_effect=execute_side_effect)
        self.fetchone = mock.MagicMock(return_value=fetchone_return)

    async def __aenter__(self) -> _MockCursor:
        return self

    async def __aexit__(self, *args: object) -> bool:
        return False


def _mock_conn(
    rowcount: int = 0,
    execute_side_effect: BaseException | None = None,
    fetchone_return: object = None,
) -> mock.MagicMock:
    """Create a mock asynch Connection.

    cursor() is a sync method returning an async context manager,
    so we use MagicMock (not AsyncMock) for the connection and set
    cursor's return_value to the _MockCursor directly.
    """
    cursor = _MockCursor(
        rowcount=rowcount,
        execute_side_effect=execute_side_effect,
        fetchone_return=fetchone_return,
    )
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor
    conn.close = mock.AsyncMock()
    conn.connect = mock.AsyncMock()
    return conn


class QueryRunnerInitTests(unittest.TestCase):
    """Tests for QueryRunner initialization."""

    def test_nodes_parsed(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='a, b, c'))
        self.assertEqual(qr.nodes, ['a', 'b', 'c'])

    def test_single_node(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='single'))
        self.assertEqual(qr.nodes, ['single'])

    def test_initial_state(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        self.assertIsNone(qr._failure)
        self.assertFalse(qr._stop_dispatch)
        self.assertIsNone(qr._progress)

    def test_failure_property(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        self.assertIsNone(qr.failure)


class QueryRunnerConnectTests(unittest.IsolatedAsyncioTestCase):
    """Tests for connect."""

    async def test_connects_all_nodes(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1,n2'))
        mock_conn = mock.AsyncMock()
        with (
            mock.patch('asynch.connection.Connection', return_value=mock_conn),
            mock.patch.object(
                qr.checkpoint_mgr, 'connect', new_callable=mock.AsyncMock
            ),
        ):
            await qr.connect()
        self.assertEqual(len(qr.connections), 2)
        self.assertIn('n1', qr.connections)
        self.assertIn('n2', qr.connections)


class QueryRunnerCloseTests(unittest.IsolatedAsyncioTestCase):
    """Tests for close."""

    async def test_close_connections(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        conn = mock.AsyncMock()
        qr.connections = {'n1': conn}
        with mock.patch.object(
            qr.checkpoint_mgr, 'close', new_callable=mock.AsyncMock
        ):
            await qr.close()
        conn.close.assert_awaited_once()

    async def test_close_handles_oserror(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        conn = mock.AsyncMock()
        conn.close.side_effect = OSError('fail')
        qr.connections = {'n1': conn}
        with mock.patch.object(
            qr.checkpoint_mgr, 'close', new_callable=mock.AsyncMock
        ):
            await qr.close()  # Should not raise

    async def test_close_cancels_poll_tasks(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        mock_task = mock.MagicMock()
        qr._poll_tasks.add(mock_task)
        qr.connections = {}
        with mock.patch.object(
            qr.checkpoint_mgr, 'close', new_callable=mock.AsyncMock
        ):
            await qr.close()
        mock_task.cancel.assert_called_once()


class QueryRunnerRunTests(unittest.IsolatedAsyncioTestCase):
    """Tests for run."""

    async def test_all_completed_returns_true(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        with mock.patch.object(
            qr.checkpoint_mgr,
            'load_completed',
            return_value={'hash1', 'hash2'},
        ):
            result = await qr.run([('hash1', 'Q1'), ('hash2', 'Q2')])
        self.assertTrue(result)

    async def test_skips_completed_queries(self) -> None:
        qr = runner.QueryRunner(
            _make_settings(concurrency=1, poll_interval=0.001, host='n1,n2')
        )
        conn = _mock_conn(rowcount=0)
        qr.connections = {'n1': conn, 'n2': conn}
        with (
            mock.patch.object(
                qr.checkpoint_mgr, 'load_completed', return_value={'hash1'}
            ),
            mock.patch.object(
                qr.checkpoint_mgr,
                'mark_completed',
                new_callable=mock.AsyncMock,
            ),
        ):
            result = await qr.run([('hash1', 'Q1'), ('hash2', 'Q2')])
        self.assertTrue(result)

    async def test_run_returns_false_on_failure(self) -> None:
        qr = runner.QueryRunner(
            _make_settings(host='n1', concurrency=1, poll_interval=0.001)
        )
        conn = _mock_conn(
            execute_side_effect=asynch_errors.ServerException(
                'syntax error', 62
            )
        )
        qr.connections = {'n1': conn}
        with (
            mock.patch.object(
                qr.checkpoint_mgr, 'load_completed', return_value=set()
            ),
            mock.patch.object(
                qr.checkpoint_mgr, 'mark_failed', new_callable=mock.AsyncMock
            ),
        ):
            result = await qr.run([('hash1', 'BAD QUERY')])
        self.assertFalse(result)
        self.assertIsNotNone(qr.failure)
        self.assertEqual(qr.failure.node, 'n1')


class QueryRunnerResetTests(unittest.IsolatedAsyncioTestCase):
    """Tests for reset."""

    async def test_delegates_to_checkpoint_mgr(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        with mock.patch.object(qr.checkpoint_mgr, 'reset', return_value=5):
            result = await qr.reset()
        self.assertEqual(result, 5)


class ExecuteQueryTests(unittest.IsolatedAsyncioTestCase):
    """Tests for _execute_query."""

    async def test_successful_execution(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(rowcount=42)
        qr.connections = {'n1': conn}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_completed', new_callable=mock.AsyncMock
        ) as mark_mock:
            await qr._execute_query('n1', 0, 'hash1', 'SELECT 1', semaphore)
        mark_mock.assert_awaited_once()
        self.assertIsNone(qr._failure)

    async def test_failed_execution_sets_failure(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(
            execute_side_effect=asynch_errors.ServerException('bad', 62)
        )
        qr.connections = {'n1': conn}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_failed', new_callable=mock.AsyncMock
        ):
            await qr._execute_query('n1', 0, 'hash1', 'BAD', semaphore)
        self.assertTrue(qr._stop_dispatch)
        self.assertIsNotNone(qr._failure)

    async def test_failed_with_cancel_on_failure(self) -> None:
        qr = runner.QueryRunner(
            _make_settings(
                host='n1', cancel_on_failure=True, poll_interval=0.001
            )
        )
        conn = _mock_conn(
            execute_side_effect=asynch_errors.ServerException('bad', 62)
        )
        qr.connections = {'n1': conn}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with (
            mock.patch.object(
                qr.checkpoint_mgr, 'mark_failed', new_callable=mock.AsyncMock
            ),
            mock.patch.object(
                qr, '_cancel_in_flight', new_callable=mock.AsyncMock
            ) as cancel_mock,
        ):
            await qr._execute_query('n1', 0, 'hash1', 'BAD', semaphore)
        cancel_mock.assert_awaited_once()

    async def test_updates_progress(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(rowcount=0)
        qr.connections = {'n1': conn}
        mock_progress = mock.MagicMock()
        qr._progress = mock_progress
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_completed', new_callable=mock.AsyncMock
        ):
            await qr._execute_query('n1', 0, 'hash1', 'Q', semaphore)
        mock_progress.mark_started.assert_called_once()
        mock_progress.mark_completed.assert_called_once()

    async def test_oserror_sets_failure(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(execute_side_effect=OSError('disconnect'))
        qr.connections = {'n1': conn}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_failed', new_callable=mock.AsyncMock
        ):
            await qr._execute_query('n1', 0, 'hash1', 'Q', semaphore)
        self.assertIsNotNone(qr.failure)
        self.assertIn('disconnect', qr.failure.error)


class PollProgressTests(unittest.IsolatedAsyncioTestCase):
    """Tests for _poll_progress."""

    async def test_cancellation(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        poll_conn = _mock_conn(fetchone_return=None)
        with mock.patch(
            'asynch.connection.Connection', return_value=poll_conn
        ):
            task = asyncio.create_task(qr._poll_progress('n1', 'qid'))
            await asyncio.sleep(0.05)
            task.cancel()
            # _poll_progress catches CancelledError and returns
            await task
        self.assertTrue(task.done())

    async def test_updates_progress_on_row(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        poll_conn = _mock_conn(fetchone_return=('qid', 500, 1000, 2.5, 50.0))
        mock_progress = mock.MagicMock()
        qr._progress = mock_progress
        with mock.patch(
            'asynch.connection.Connection', return_value=poll_conn
        ):
            task = asyncio.create_task(qr._poll_progress('n1', 'qid'))
            await asyncio.sleep(0.05)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        mock_progress.update_query.assert_called()

    async def test_handles_server_error(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        poll_conn = _mock_conn(
            execute_side_effect=asynch_errors.ServerException('err', 62)
        )
        with mock.patch(
            'asynch.connection.Connection', return_value=poll_conn
        ):
            task = asyncio.create_task(qr._poll_progress('n1', 'qid'))
            await asyncio.sleep(0.05)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_closes_connection_on_cancel(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        poll_conn = _mock_conn(fetchone_return=None)
        with mock.patch(
            'asynch.connection.Connection', return_value=poll_conn
        ):
            task = asyncio.create_task(qr._poll_progress('n1', 'qid'))
            await asyncio.sleep(0.05)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        poll_conn.close.assert_awaited_once()


class CancelInFlightTests(unittest.IsolatedAsyncioTestCase):
    """Tests for _cancel_in_flight."""

    async def test_sends_kill_query(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1'))
        conn = _mock_conn()
        qr.connections = {'n1': conn}
        await qr._cancel_in_flight()
        cursor = conn.cursor.return_value
        cursor.execute.assert_awaited_once()

    async def test_handles_error(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1'))
        conn = _mock_conn(execute_side_effect=OSError('fail'))
        qr.connections = {'n1': conn}
        await qr._cancel_in_flight()  # Should not raise


class QueryFailureTests(unittest.TestCase):
    """Tests for _QueryFailure."""

    def test_fields(self) -> None:
        f = runner._QueryFailure(
            query_text='SELECT 1', node='n1', error='bad', offset=3
        )
        self.assertEqual(f.query_text, 'SELECT 1')
        self.assertEqual(f.node, 'n1')
        self.assertEqual(f.error, 'bad')
        self.assertEqual(f.offset, 3)
