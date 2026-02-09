"""Tests for configuration management."""

from __future__ import annotations

import os
import unittest


class RunnerSettingsTests(unittest.TestCase):
    """Tests for RunnerSettings."""

    def _make_settings(self, **overrides: object) -> object:
        """Create settings with required fields populated."""
        import sys

        from clickhouse_query_runner import settings

        defaults = {
            'CLICKHOUSE_HOST': 'localhost',
            'CLICKHOUSE_DATABASE': 'default',
            'CLICKHOUSE_USER': 'user',
            'CLICKHOUSE_PASSWORD': 'pass',
        }
        defaults.update(overrides)
        env = {
            k: str(v)
            for k, v in defaults.items()
            if k.startswith(('CLICKHOUSE_', 'VALKEY_'))
        }
        orig_env = os.environ.copy()
        orig_argv = sys.argv[:]
        try:
            os.environ.update(env)
            sys.argv = ['clickhouse-query-runner', 'test.sql']
            return settings.RunnerSettings()
        finally:
            os.environ.clear()
            os.environ.update(orig_env)
            sys.argv = orig_argv

    def test_defaults(self) -> None:
        s = self._make_settings()
        self.assertEqual(s.port, 9440)
        self.assertTrue(s.secure)
        self.assertEqual(s.concurrency, 2)
        self.assertIsNone(s.run_id)
        self.assertEqual(s.valkey_url, 'redis://localhost:6379/0')
        self.assertEqual(s.checkpoint_ttl, 604800)
        self.assertEqual(s.poll_interval, 0.5)
        self.assertFalse(s.cancel_on_failure)
        self.assertFalse(s.dry_run)
        self.assertFalse(s.reset)
        self.assertFalse(s.verbose)

    def test_env_prefix(self) -> None:
        s = self._make_settings(CLICKHOUSE_HOST='myhost')
        self.assertEqual(s.host, 'myhost')

    def test_valkey_url_env(self) -> None:
        s = self._make_settings(VALKEY_URL='redis://custom:6379/1')
        self.assertEqual(s.valkey_url, 'redis://custom:6379/1')

    def test_password_is_secret(self) -> None:
        s = self._make_settings(CLICKHOUSE_PASSWORD='test-pw')  # noqa: S106
        self.assertEqual(s.password.get_secret_value(), 'test-pw')
        self.assertNotIn('test-pw', str(s.password))

    def test_query_file_positional(self) -> None:
        s = self._make_settings()
        self.assertEqual(s.query_file, 'test.sql')

    def test_valkey_url_cli_flag(self) -> None:
        import sys

        from clickhouse_query_runner import settings

        orig_env = os.environ.copy()
        orig_argv = sys.argv[:]
        try:
            os.environ.update(
                {
                    'CLICKHOUSE_HOST': 'localhost',
                    'CLICKHOUSE_DATABASE': 'default',
                    'CLICKHOUSE_USER': 'user',
                    'CLICKHOUSE_PASSWORD': 'pass',
                }
            )
            sys.argv = [
                'clickhouse-query-runner',
                '--valkey-url',
                'redis://cli-flag:6379/2',
                'test.sql',
            ]
            s = settings.RunnerSettings()
            self.assertEqual(s.valkey_url, 'redis://cli-flag:6379/2')
        finally:
            os.environ.clear()
            os.environ.update(orig_env)
            sys.argv = orig_argv

    def test_concurrency_validation_zero(self) -> None:
        import pydantic

        with self.assertRaises(pydantic.ValidationError):
            self._make_settings(CLICKHOUSE_CONCURRENCY='0')

    def test_concurrency_validation_negative(self) -> None:
        import sys

        import pydantic

        from clickhouse_query_runner import settings

        orig_env = os.environ.copy()
        orig_argv = sys.argv[:]
        try:
            os.environ.update(
                {
                    'CLICKHOUSE_HOST': 'localhost',
                    'CLICKHOUSE_DATABASE': 'default',
                    'CLICKHOUSE_USER': 'user',
                    'CLICKHOUSE_PASSWORD': 'pass',
                    'CLICKHOUSE_CONCURRENCY': '-1',
                }
            )
            sys.argv = ['clickhouse-query-runner', 'test.sql']
            with self.assertRaises((SystemExit, pydantic.ValidationError)):
                settings.RunnerSettings()
        finally:
            os.environ.clear()
            os.environ.update(orig_env)
            sys.argv = orig_argv
