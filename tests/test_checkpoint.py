"""Tests for Valkey checkpoint management."""

from __future__ import annotations

import json
import time
import unittest
from unittest import mock

import valkey.exceptions

from clickhouse_query_runner import checkpoint


class CheckpointManagerInitTests(unittest.TestCase):
    """Tests for CheckpointManager initialization."""

    def test_explicit_run_id(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost',
            run_id='custom-id',
            file_path='/some/file.sql',
        )
        self.assertEqual(mgr.run_id, 'custom-id')

    def test_derived_run_id(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost',
            run_id=None,
            file_path='/some/file.sql',
        )
        self.assertEqual(len(mgr.run_id), 16)

    def test_derived_run_id_deterministic(self) -> None:
        mgr1 = checkpoint.CheckpointManager(
            valkey_url='redis://localhost',
            run_id=None,
            file_path='/same/path.sql',
        )
        mgr2 = checkpoint.CheckpointManager(
            valkey_url='redis://localhost',
            run_id=None,
            file_path='/same/path.sql',
        )
        self.assertEqual(mgr1.run_id, mgr2.run_id)

    def test_custom_ttl(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost',
            run_id='x',
            file_path='f.sql',
            ttl=3600,
        )
        self.assertEqual(mgr.ttl, 3600)

    def test_default_ttl(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        self.assertEqual(mgr.ttl, 604800)


class CheckpointManagerKeyTests(unittest.TestCase):
    """Tests for key generation."""

    def test_key_format(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run123', file_path='f.sql'
        )
        self.assertEqual(mgr._key('abc'), 'cqr:run123:abc')


class CheckpointManagerConnectTests(unittest.IsolatedAsyncioTestCase):
    """Tests for connect."""

    async def test_successful_connect(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        mock_client = mock.AsyncMock()
        with mock.patch('valkey.asyncio.from_url', return_value=mock_client):
            await mgr.connect()
        mock_client.ping.assert_awaited_once()
        self.assertTrue(mgr._available)

    async def test_connect_failure(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        mock_client = mock.AsyncMock()
        mock_client.ping.side_effect = valkey.exceptions.ConnectionError(
            'refused'
        )
        with mock.patch('valkey.asyncio.from_url', return_value=mock_client):
            await mgr.connect()
        self.assertFalse(mgr._available)

    async def test_connect_oserror(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        mock_client = mock.AsyncMock()
        mock_client.ping.side_effect = OSError('network')
        with mock.patch('valkey.asyncio.from_url', return_value=mock_client):
            await mgr.connect()
        self.assertFalse(mgr._available)


class CheckpointManagerCloseTests(unittest.IsolatedAsyncioTestCase):
    """Tests for close."""

    async def test_close_with_client(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        mgr.client = mock.AsyncMock()
        await mgr.close()
        mgr.client.aclose.assert_awaited_once()

    async def test_close_without_client(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        await mgr.close()  # Should not raise


class CheckpointManagerLoadCompletedTests(unittest.IsolatedAsyncioTestCase):
    """Tests for load_completed."""

    async def test_unavailable_returns_empty(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = False
        result = await mgr.load_completed()
        self.assertEqual(result, set())

    async def test_loads_completed_hashes(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = True
        mock_client = mock.AsyncMock()
        mock_client.scan.return_value = (
            0,
            [b'cqr:run1:hash1', b'cqr:run1:hash2'],
        )
        mock_client.get.side_effect = [
            json.dumps({'status': 'completed'}).encode(),
            json.dumps({'status': 'failed'}).encode(),
        ]
        mgr.client = mock_client
        result = await mgr.load_completed()
        self.assertEqual(result, {'hash1'})

    async def test_skips_none_values(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = True
        mock_client = mock.AsyncMock()
        mock_client.scan.return_value = (0, [b'cqr:run1:hash1'])
        mock_client.get.return_value = None
        mgr.client = mock_client
        result = await mgr.load_completed()
        self.assertEqual(result, set())

    async def test_handles_valkey_error(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = True
        mgr._last_connected = time.monotonic()
        mock_client = mock.AsyncMock()
        mock_client.scan.side_effect = valkey.exceptions.ConnectionError(
            'down'
        )
        mgr.client = mock_client
        result = await mgr.load_completed()
        self.assertEqual(result, set())


class CheckpointManagerWriteTests(unittest.IsolatedAsyncioTestCase):
    """Tests for mark_completed and mark_failed."""

    async def test_mark_completed(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost',
            run_id='run1',
            file_path='f.sql',
            ttl=100,
        )
        mgr._available = True
        mock_client = mock.AsyncMock()
        mgr.client = mock_client
        await mgr.mark_completed(
            query_hash='abc',
            node='node1',
            started_at='2026-01-01T00:00:00',
            completed_at='2026-01-01T00:00:10',
            rows_read=1000,
            elapsed=10.123,
        )
        mock_client.setex.assert_awaited_once()
        args = mock_client.setex.call_args
        self.assertEqual(args[0][0], 'cqr:run1:abc')
        self.assertEqual(args[0][1], 100)
        data = json.loads(args[0][2])
        self.assertEqual(data['status'], 'completed')
        self.assertEqual(data['rows_read'], 1000)
        self.assertEqual(data['elapsed'], 10.12)

    async def test_mark_failed(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = True
        mock_client = mock.AsyncMock()
        mgr.client = mock_client
        await mgr.mark_failed(
            query_hash='abc',
            node='node1',
            started_at='2026-01-01T00:00:00',
            error='syntax error',
        )
        mock_client.setex.assert_awaited_once()
        data = json.loads(mock_client.setex.call_args[0][2])
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'syntax error')

    async def test_write_when_unavailable(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = False
        mock_client = mock.AsyncMock()
        mgr.client = mock_client
        await mgr.mark_completed(
            query_hash='abc',
            node='node1',
            started_at='t0',
            completed_at='t1',
            rows_read=0,
            elapsed=0.0,
        )
        mock_client.setex.assert_not_awaited()

    async def test_write_valkey_error_checks_availability(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = True
        mgr._last_connected = time.monotonic()
        mock_client = mock.AsyncMock()
        mock_client.setex.side_effect = valkey.exceptions.ConnectionError(
            'down'
        )
        mgr.client = mock_client
        # Should not raise since last_connected is recent
        await mgr._write('abc', {'status': 'completed'})


class CheckpointManagerResetTests(unittest.IsolatedAsyncioTestCase):
    """Tests for reset."""

    async def test_reset_deletes_keys(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = True
        mock_client = mock.AsyncMock()
        mock_client.scan.return_value = (0, [b'cqr:run1:a', b'cqr:run1:b'])
        mock_client.delete.return_value = 2
        mgr.client = mock_client
        result = await mgr.reset()
        self.assertEqual(result, 2)
        mock_client.delete.assert_awaited_once()

    async def test_reset_unavailable(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = False
        result = await mgr.reset()
        self.assertEqual(result, 0)

    async def test_reset_no_keys(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = True
        mock_client = mock.AsyncMock()
        mock_client.scan.return_value = (0, [])
        mgr.client = mock_client
        result = await mgr.reset()
        self.assertEqual(result, 0)
        mock_client.delete.assert_not_awaited()

    async def test_reset_handles_error(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='run1', file_path='f.sql'
        )
        mgr._available = True
        mock_client = mock.AsyncMock()
        mock_client.scan.side_effect = valkey.exceptions.ConnectionError(
            'down'
        )
        mgr.client = mock_client
        result = await mgr.reset()
        self.assertEqual(result, 0)


class CheckAvailabilityTests(unittest.TestCase):
    """Tests for _check_availability."""

    def test_raises_after_30s(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        mgr._available = True
        mgr._last_connected = time.monotonic() - 31
        with self.assertRaises(OSError):
            mgr._check_availability()
        self.assertFalse(mgr._available)

    def test_does_not_raise_within_30s(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        mgr._available = True
        mgr._last_connected = time.monotonic()
        mgr._check_availability()  # Should not raise

    def test_does_not_raise_when_already_unavailable(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        mgr._available = False
        mgr._last_connected = time.monotonic() - 100
        mgr._check_availability()  # Should not raise

    def test_does_not_raise_when_never_connected(self) -> None:
        mgr = checkpoint.CheckpointManager(
            valkey_url='redis://localhost', run_id='x', file_path='f.sql'
        )
        mgr._available = True
        mgr._last_connected = 0.0
        mgr._check_availability()  # Should not raise
